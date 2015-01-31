package org.apache.jackrabbit.oak.plugins.document.elastic;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.*;
import org.apache.jackrabbit.oak.plugins.document.cache.CachingDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.cache.ForwardingListener;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocOffHeapCache;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.operation.plain.Preference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A document store that uses ElasticSearch as the backend.
 */
public class ElasticDocumentStore implements CachingDocumentStore {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticDocumentStore.class);

  private static final boolean LOG_TIME = false;

  private static enum DocumentReadPreference {
    PRIMARY,
    PREFER_PRIMARY,
    PREFER_SECONDARY,
    PREFER_SECONDARY_IF_OLD_ENOUGH
  }

  private final Client client;

  private final long maxReplicationLagMillis;
  private final long maxDeltaForModTimeIdxSecs =
    Long.getLong("oak.elastic.maxDeltaForModTimeIdxSecs", -1);

  private final Cache<CacheValue, NodeDocument> nodesCache;
  private final CacheStats cacheStats;

  private final Comparator<Revision> comparator = StableRevisionComparator.REVERSE;

  private long timeSum;
  private Clock clock = Clock.SIMPLE;

  private final static class TreeLock {

    private final Lock parentLock;
    private final Lock lock;

    private TreeLock(Lock parentLock, Lock lock) {
      this.parentLock = parentLock;
      this.lock = lock;
    }

    static TreeLock shared(ReadWriteLock parentLock, Lock lock) {
      return new TreeLock(parentLock.readLock(), lock).lock();
    }

    static TreeLock exclusive(ReadWriteLock parentLock) {
      return new TreeLock(parentLock.writeLock(), null).lock();
    }

    private TreeLock lock() {
      parentLock.lock();
      if (lock != null) {
        lock.lock();
      }
      return this;
    }

    private void unlock() {
      if (lock != null) {
        lock.unlock();
      }
      parentLock.unlock();
    }
  }

  /**
   * Acquires a log for the given key. The returned tree lock will also hold
   * a shared lock on the parent key.
   *
   * @param key a key.
   * @return the acquired lock for the given key.
   */
  private TreeLock acquire(String key) {
    return TreeLock.shared(parentLocks.get(getParentId(key)), locks.get(key));
  }

  /**
   * Acquires an exclusive lock on the given parent key. Use this method to
   * block cache access for child keys of the given parent key.
   *
   * @param parentKey the parent key.
   * @return the acquired lock for the given parent key.
   */
  private TreeLock acquireExclusive(String parentKey) {
    return TreeLock.exclusive(parentLocks.get(parentKey));
  }


  public ElasticDocumentStore(Client client, DocumentMK.Builder builder) {
    checkVersion(client);
    this.client = client;

    maxReplicationLagMillis = builder.getMaxReplicationLagMillis();

    // TODO expire entries if the parent was changed
    if (builder.useOffHeapCache()) {
      nodesCache = createOffHeapCache(builder);
    } else {
      nodesCache = builder.buildDocumentCache(this);
    }

    cacheStats = new CacheStats(nodesCache, "Document-Documents", builder.getWeigher(),
      builder.getDocumentCacheSize());
    LOG.info("Configuration maxReplicationLagMillis {}, " +
      "maxDeltaForModTimeIdxSecs {}", maxReplicationLagMillis, maxDeltaForModTimeIdxSecs);
  }

  private static void log(String message, Object... args) {
    if (LOG.isDebugEnabled()) {
      String argList = Arrays.toString(args);
      if (argList.length() > 10000) {
        argList = argList.length() + ": " + argList;
      }
      LOG.debug(message + argList);
    }
  }

  private static void checkVersion(Client client) {
    NodesInfoResponse response = client
      .admin()
      .cluster()
      .prepareNodesInfo()
      .all()
      .execute()
      .actionGet();

    for (NodeInfo info : response.getNodes()) {
      Version version = info.getVersion();
      if (version.major > 1) {
        continue;
      }
      if (version.minor < 4) {
        String msg = "Elastic Search version 1.4.2 or higher required. " +
          "Currently connected to a Elastic Search with version: " + version;
        throw new RuntimeException(msg);
      }
    }
  }

  private static long start() {
    return LOG_TIME ? System.currentTimeMillis() : 0;
  }

  private void end(String message, long start) {
    if (LOG_TIME) {
      long t = System.currentTimeMillis() - start;
      if (t > 0) {
        LOG.debug(message + ": " + t);
      }
      timeSum += t;
    }
  }

  private long getTime() {
    return clock.getTime();
  }

  private void setClock(Clock clock) {
    this.clock = clock;
  }

  private Cache<CacheValue, NodeDocument> createOffHeapCache(
    DocumentMK.Builder builder) {
    ForwardingListener<CacheValue, NodeDocument> listener = ForwardingListener.newInstance();

    Cache<CacheValue, NodeDocument> primaryCache = CacheBuilder.newBuilder()
      .weigher(builder.getWeigher())
      .maximumWeight(builder.getDocumentCacheSize())
      .removalListener(listener)
      .recordStats()
      .build();

    return new NodeDocOffHeapCache(primaryCache, listener, builder, this);
  }

  private <T extends Document> String getElasticReadPreference(final Collection<T> collection,
                                                               final String parentId,
                                                               final DocumentReadPreference preference) {
    switch (preference) {
      case PRIMARY:
        return Preference.PRIMARY.toString();
      case PREFER_PRIMARY:
        return Preference.PRIMARY_FIRST.toString();
      case PREFER_SECONDARY:
        return getConfiguredReadPreference();
      case PREFER_SECONDARY_IF_OLD_ENOUGH:
        if (collection != Collection.NODES) {
          return Preference.PRIMARY.toString();
        }

        String readPreference = Preference.PRIMARY.toString();
        if (parentId != null) {
          long replicationSafeLimit = getTime() - maxReplicationLagMillis;
          NodeDocument cachedDoc = (NodeDocument) getIfCached(collection, parentId);

          if (cachedDoc != null && !cachedDoc.hasBeenModifiedSince(replicationSafeLimit)) {
            readPreference = getConfiguredReadPreference();
          }
        }
        return readPreference;
      default:
        throw new IllegalArgumentException("Unsupported usage " + preference);
    }
  }

  private String getConfiguredReadPreference() {
    NodesInfoResponse response = client
      .admin()
      .cluster()
      .prepareNodesInfo()
      .all()
      .execute()
      .actionGet();

    NodeInfo info = Iterables.getLast(
      Arrays.asList(response.getNodes()));

    return Preference.ONLY_NODE.toString().concat(
      ":".concat(info.getNode().getId()));
  }

  @Override
  public CacheStats getCacheStats() {
    return cacheStats;
  }

  @Override
  public <T extends Document> T find(Collection<T> collection, String key) {
    //return find(collection, key, true, -1);
    return null;
  }

  @Override
  public <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge) {
    return null;
  }

  private <T extends Document> T find(final Collection<T> collection,
                                      final String key,
                                      boolean preferCached,
                                      final int maxCacheAge) {
    if (collection != Collection.NODES) {
      return findUncachedWithRetry(collection, key,
        DocumentReadPreference.PRIMARY, 2);
    }

    CacheValue cacheKey = new StringValue(key);
    NodeDocument doc;
    if (maxCacheAge > 0 || preferCached) {
      // first try without lock
      doc = nodesCache.getIfPresent(cacheKey);
      if (doc != null) {
        if (preferCached ||
          getTime() - doc.getCreated() < maxCacheAge) {
          if (doc == NodeDocument.NULL) {
            return null;
          }
          return (T) doc;
        }
      }
    }
    Throwable t;
    try {
      TreeLock lock = acquire(key);
      try {
        if (maxCacheAge == 0) {
          invalidateCache(collection, key);
        }
        while (true) {
          doc = nodesCache.get(cacheKey, new Callable<NodeDocument>() {
            @Override
            public NodeDocument call() throws Exception {
              NodeDocument doc = (NodeDocument) findUncachedWithRetry(
                collection, key,
                getReadPreference(maxCacheAge), 2);
              if (doc == null) {
                doc = NodeDocument.NULL;
              }
              return doc;
            }
          });
          if (maxCacheAge == 0 || preferCached) {
            break;
          }
          if (getTime() - doc.getCreated() < maxCacheAge) {
            break;
          }
          // too old: invalidate, try again
          invalidateCache(collection, key);
        }
      } finally {
        lock.unlock();
      }
      if (doc == NodeDocument.NULL) {
        return null;
      } else {
        return (T) doc;
      }
    } catch (UncheckedExecutionException e) {
      t = e.getCause();
    } catch (ExecutionException e) {
      t = e.getCause();
    }
    throw new DocumentStoreException("Failed to load document with " + key, t);

    return null;
  }

  @CheckForNull
  private <T extends Document> T findUncachedWithRetry(
    Collection<T> collection, String key,
    DocumentReadPreference docReadPref,
    int retries) {
    checkArgument(retries >= 0, "retries must not be negative");
    int numAttempts = retries + 1;
    Exception ex = null;
    for (int i = 0; i < numAttempts; i++) {
      if (i > 0) {
        LOG.warn("Retrying read of " + key);
      }
      try {
        return findUncached(collection, key, docReadPref);
      } catch (Exception e) {
        ex = e;
      }
    }
    if (ex != null) {
      throw new IllegalStateException(ex);
    } else {
      throw new IllegalStateException();
    }
  }

  @CheckForNull
  protected <T extends Document> T findUncached(final Collection<T> collection,
                                                final String key,
                                                final DocumentReadPreference docReadPref) {
    log("findUncached", key, docReadPref);
    long start = start();
    try {
      String readPreference = getElasticReadPreference(collection, Utils.getParentId(key), docReadPref);

      if (Preference.parse(readPreference) == Preference.ONLY_NODE) {
        LOG.trace("Routing call to secondary for fetching [{}]", key);
      }

      GetResponse response = new GetRequestBuilder(client, collection.toString())
        .setPreference(readPreference)
        .setId(key)
        .execute()
        .actionGet();

      if (response == null
        && Preference.parse(readPreference) == Preference.ONLY_NODE) {
        response = new GetRequestBuilder(client, collection.toString())
          .setPreference(Preference.PRIMARY.toString())
          .setId(key)
          .execute()
          .actionGet();
      }
      if (response == null) {
        return null;
      }
      T doc = convertFromDBObject(collection, response);
      if (doc != null) {
        doc.seal();
      }
      return doc;
    } finally {
      end("findUncached", start);
    }
  }

  @CheckForNull
  protected <T extends Document> T convertFromDBObject(@Nonnull final Collection<T> collection,
                                                       @Nullable final GetResponse response) {
    T copy = null;
    if (response != null) {
      copy = collection.newDocument(this);
      Map<String, Object> responseMap = response.getSourceAsMap();
      for (String key : responseMap.keySet()) {
        Object o = responseMap.get(key);
        if (o instanceof String) {
          copy.put(key, o);
        } else if (o instanceof Long) {
          copy.put(key, o);
        } else if (o instanceof Integer) {
          copy.put(key, o);
        } else if (o instanceof Boolean) {
          copy.put(key, o);
        } else if (o instanceof ArrayList<?>) {
          copy.put(key, o);
        } else if (o instanceof HashMap<?, ?>) {
          copy.put(key, o);
        }
      }
    }
    return copy;
  }

  @Nonnull
  @Override
  public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, int limit) {
    return null;
  }

  @Nonnull
  @Override
  public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, String indexedProperty, long startValue, int limit) {
    return null;
  }

  @Override
  public <T extends Document> void remove(Collection<T> collection, String key) {

  }

  @Override
  public <T extends Document> void remove(Collection<T> collection, List<String> keys) {

  }

  @Override
  public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps) {
    return false;
  }

  @Override
  public <T extends Document> void update(Collection<T> collection, List<String> keys, UpdateOp updateOp) {

  }

  @Override
  public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update) {
    return null;
  }

  @Override
  public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update) {
    return null;
  }

  @Override
  public void invalidateCache() {

  }

  @Override
  public <T extends Document> void invalidateCache(Collection<T> collection, String key) {

  }

  @Override
  public void dispose() {

  }

  @Override
  public <T extends Document> T getIfCached(Collection<T> collection, String key) {
    if (collection != Collection.NODES) {
      return null;
    }
    @SuppressWarnings("unchecked")
    T doc = (T) nodesCache.getIfPresent(new StringValue(key));
    return doc;
  }

  @Override
  public void setReadWriteMode(String readWriteMode) {

  }
}
