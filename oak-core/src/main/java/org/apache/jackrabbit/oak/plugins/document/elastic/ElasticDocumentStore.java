package org.apache.jackrabbit.oak.plugins.document.elastic;

import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Striped;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.*;
import org.apache.jackrabbit.oak.plugins.document.cache.CachingDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.cache.ForwardingListener;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocOffHeapCache;
import org.apache.jackrabbit.oak.plugins.document.mongo.RevisionEntry;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.operation.plain.Preference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * A document store that uses ElasticSearch as the backend.
 */
public class ElasticDocumentStore implements CachingDocumentStore {

  private static final String ID = "id";

  private static final String REVISIONS = "_revisions";

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

  private final Striped<Lock> locks = Striped.lock(128);
  private final Striped<ReadWriteLock> parentLocks = Striped.readWriteLock(64);

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

  private static <T extends Document> String getIndexName(Collection<T> collection) {
    StringBuilder builder = new StringBuilder();
    for (char c : collection.toString().toCharArray()) {
      if (Character.isUpperCase(c)) {
        builder.append('_');
        builder.append(Character.toLowerCase(c));
      } else {
        builder.append(c);
      }
    }
    return builder.toString();
  }

  private static <T extends Document> String getTypeName(Collection<T> collection) {
    String index = getIndexName(collection);
    return index.substring(0, index.length() - 1).toLowerCase();
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

  private TreeLock acquire(String key) {
    return TreeLock.shared(parentLocks.get(getParentId(key)), locks.get(key));
  }

  private TreeLock acquireExclusive(String parentKey) {
    return TreeLock.exclusive(parentLocks.get(parentKey));
  }

  @Nonnull
  private static String getParentId(@Nonnull String id) {
    String parentId = Utils.getParentId(checkNotNull(id));
    if (parentId == null) {
      parentId = "";
    }
    return parentId;
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

  private <T extends Document> void applyToCache(@Nonnull Collection<T> collection,
                                                 @Nullable T oldDoc,
                                                 @Nonnull UpdateOp updateOp) {
    // cache the new document
    if (collection == Collection.NODES) {
      CacheValue key = new StringValue(updateOp.getId());
      NodeDocument newDoc = (NodeDocument) collection.newDocument(this);
      if (oldDoc != null) {
        // we can only update the cache based on the oldDoc if we
        // still have the oldDoc in the cache, otherwise we may
        // update the cache with an outdated document
        NodeDocument cached = nodesCache.getIfPresent(key);
        if (cached == null) {
          // cannot use oldDoc to update cache
          return;
        }
        oldDoc.deepCopy(newDoc);
      }
      UpdateUtils.applyChanges(newDoc, updateOp, comparator);
      newDoc.seal();

      NodeDocument cached = addToCache(newDoc);
      if (cached == newDoc) {
        // successful
        return;
      }
      if (oldDoc == null) {
        // this is an insert and some other thread was quicker
        // loading it into the cache -> return now
        return;
      }
      // this is an update (oldDoc != null)
      if (Objects.equal(cached.getModCount(), oldDoc.getModCount())) {
        nodesCache.put(key, newDoc);
      } else {
        // the cache entry was modified by some other thread in
        // the meantime. the updated cache entry may or may not
        // include this update. we cannot just apply our update
        // on top of the cached entry.
        // therefore we must invalidate the cache entry
        nodesCache.invalidate(key);
      }
    }
  }

  @Nonnull
  private NodeDocument addToCache(@Nonnull final NodeDocument doc) {
    if (doc == NodeDocument.NULL) {
      throw new IllegalArgumentException("doc must not be NULL document");
    }
    doc.seal();
    // make sure we only cache the document if it wasn't
    // changed and cached by some other thread in the
    // meantime. That is, use get() with a Callable,
    // which is only used when the document isn't there
    try {
      CacheValue key = new StringValue(doc.getId());
      for (; ; ) {
        NodeDocument cached = nodesCache.get(key,
            new Callable<NodeDocument>() {
              @Override
              public NodeDocument call() {
                return doc;
              }
            });
        if (cached != NodeDocument.NULL) {
          return cached;
        } else {
          nodesCache.invalidate(key);
        }
      }
    } catch (ExecutionException e) {
      // will never happen because call() just returns
      // the already available doc
      throw new IllegalStateException(e);
    }
  }

  private DocumentReadPreference getReadPreference(int maxCacheAge) {
    if (maxCacheAge >= 0 && maxCacheAge < maxReplicationLagMillis) {
      return DocumentReadPreference.PRIMARY;
    } else if (maxCacheAge == Integer.MAX_VALUE) {
      return DocumentReadPreference.PREFER_SECONDARY;
    } else {
      return DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH;
    }
  }

  private <T extends Document> String getElasticReadPreference(final Collection<T> collection,
                                                               final String parentId,
                                                               final DocumentReadPreference preference) {
    switch (preference) {
      case PRIMARY:
        return Preference.PRIMARY.type();
      case PREFER_PRIMARY:
        return Preference.PRIMARY_FIRST.type();
      case PREFER_SECONDARY:
        return getConfiguredReadPreference();
      case PREFER_SECONDARY_IF_OLD_ENOUGH:
        if (collection != Collection.NODES) {
          return Preference.PRIMARY.type();
        }

        String readPreference = Preference.PRIMARY.type();
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

    return Preference.ONLY_NODE.type().concat(
        ":".concat(info.getNode().getId()));
  }

  @Override
  public CacheStats getCacheStats() {
    return cacheStats;
  }

  @Override
  public <T extends Document> T find(Collection<T> collection, String key) {
    return find(collection, key, true, -1);
  }

  @Override
  public <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge) {
    return find(collection, key, false, maxCacheAge);
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
      } catch (IndexMissingException e) {
        return null;
      } catch (Exception e) {
        ex = e;
      }
    }

    if (ex != null) {
      throw new IllegalStateException();
    }
    return null;
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

      GetResponse response = new GetRequestBuilder(client, getIndexName(collection))
          .setPreference(readPreference)
          .setId(key)
          .execute()
          .actionGet();

      if (response == null
          && Preference.parse(readPreference) == Preference.ONLY_NODE) {
        response = new GetRequestBuilder(client, getIndexName(collection))
            .setPreference(Preference.PRIMARY.toString())
            .setId(key)
            .execute()
            .actionGet();
      }
      if (response == null) {
        return null;
      }
      T doc = convertFromMap(collection, response.getSourceAsMap());
      if (doc != null) {
        doc.seal();
      }
      return doc;
    } finally {
      end("findUncached", start);
    }
  }

  @CheckForNull
  protected <T extends Document> T convertFromMap(@Nonnull final Collection<T> collection,
                                                  @Nullable final Map<String, Object> sourceMap) {
    T copy = null;
    if (sourceMap != null) {
      copy = collection.newDocument(this);
      for (String key : sourceMap.keySet()) {
        Object o = sourceMap.get(key);
        if (o instanceof String) {
          try {
            JsopTokenizer t = new JsopTokenizer(o.toString());
            t.read('{');
            JsonObject json = JsonObject.create(t);
            Map<String, Object> props = new TreeMap<String, Object>();
            props.putAll(json.getProperties());
            props.putAll(json.getChildren());

            if (key == REVISIONS) {
              copy.put(key, convertFromMap(props));
            } else {
              copy.put(key, props);
            }
          } catch (IllegalArgumentException exp) {
            copy.put(key, o);
          }
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
  private Map<Revision, Object> convertFromMap(@Nonnull final Map<String, Object> sourceMap) {
    Map<Revision, Object> map = new TreeMap<Revision, Object>(comparator);
    for (Map.Entry<String, Object> entry : sourceMap.entrySet()) {
      map.put(Revision.fromString(entry.getKey()), entry.getValue());
    }
    return map;
  }

  @CheckForNull
  private <T extends Document> T findAndModify(Collection<T> collection,
                                               UpdateOp updateOp,
                                               boolean upsert,
                                               boolean checkConditions) {
    updateOp = updateOp.copy();
    XContentBuilder update = createUpdate(updateOp);

    String id = updateOp.getId();
    TreeLock lock = acquire(id);
    long start = start();
    try {
      // get modCount of cached document
      Number modCount = null;
      T cachedDoc = null;
      if (collection == Collection.NODES) {
        @SuppressWarnings("unchecked")
        T doc = (T) nodesCache.getIfPresent(new StringValue(updateOp.getId()));
        cachedDoc = doc;
        if (cachedDoc != null) {
          modCount = cachedDoc.getModCount();
        }
      }

      UpdateRequest updateRequest = new UpdateRequest();
      updateRequest.index(getIndexName(collection));
      updateRequest.type(getTypeName(collection));
      updateRequest.doc(update);

      // perform a conditional update with limited result
      // if we have a matching modCount
      if (modCount != null) {
        updateRequest.id("1");
        UpdateResponse response = client.update(updateRequest).get();

        if (response != null) {
          // success, update cached document
          applyToCache(collection, cachedDoc, updateOp);
          // return previously cached document
          return cachedDoc;
        }
      }

      // conditional update failed or not possible
      // perform operation and get complete document
      if (upsert) {
        IndexRequest indexRequest = new IndexRequest(getIndexName(collection), getTypeName(collection), id)
            .source(update);
        updateRequest.upsert(indexRequest);
        updateRequest.getFromContext(id);
      }
      updateRequest.id(id);
      UpdateResponse response = client.update(updateRequest).get();
      if (checkConditions && response == null) {
        return null;
      }
      T oldDoc = find(collection, id);
      applyToCache(collection, oldDoc, updateOp);
      if (oldDoc != null) {
        oldDoc.seal();
      }
      return oldDoc;
    } catch (Exception e) {
      throw DocumentStoreException.convert(e);
    } finally {
      lock.unlock();
      end("findAndModify", start);
    }
  }

  @Nonnull
  private static XContentBuilder createUpdate(UpdateOp updateOp) {
    try {
      XContentBuilder update = jsonBuilder().startObject();
      updateOp.increment(Document.MOD_COUNT, 1);

      for (Map.Entry<UpdateOp.Key, UpdateOp.Operation> entry : updateOp.getChanges().entrySet()) {
        UpdateOp.Key k = entry.getKey();
        if (k.getName().equals(Document.ID)) {
          update.field(ID, updateOp.getId());
          continue;
        }
        UpdateOp.Operation op = entry.getValue();
        switch (op.type) {
          case SET:
          case SET_MAP_ENTRY: {
            update.field(k.toString(), op.value);
            break;
          }
          case MAX: {
            update.field(k.toString(), op.value);
            break;
          }
          case INCREMENT: {
            update.field(k.toString(), op.value);
            break;
          }
          case REMOVE_MAP_ENTRY: {
            update.field(k.toString(), "1");
            break;
          }
        }
      }

      return update.endObject();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  @Override
  public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, int limit) {
    return query(collection, fromKey, toKey, null, 0, limit);
  }

  @Nonnull
  @Override
  public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, String indexedProperty, long startValue, int limit) {
    log("query", fromKey, toKey, indexedProperty, startValue, limit);

    SearchHit[] hits = new SearchHit[]{};
    try {
      SearchResponse response = client.prepareSearch(getIndexName(collection))
          .setSearchType(SearchType.DEFAULT)
          .setQuery(QueryBuilders.filteredQuery(
              QueryBuilders.matchAllQuery(),
              new RangeFilterBuilder(ID).gte(fromKey).lte(toKey)
          ))
          .execute()
          .actionGet();
      hits = response.getHits().getHits();
    } catch (IndexMissingException e) {
    }

    String parentId = Utils.getParentIdFromLowerLimit(fromKey);
    TreeLock lock = acquireExclusive(parentId != null ? parentId : "");
    long start = start();
    try {
      List<T> list = new ArrayList<T>();
      for (int i = 0; i < limit && i < hits.length; i++) {

        T doc = convertFromMap(collection, hits[i].sourceAsMap());
        if (collection == Collection.NODES && doc != null) {
          doc.seal();
          String id = doc.getId();
          CacheValue cacheKey = new StringValue(id);
          NodeDocument cached = nodesCache.getIfPresent(cacheKey);
          if (cached != null && cached != NodeDocument.NULL) {
            // check mod count
            Number cachedModCount = cached.getModCount();
            Number modCount = doc.getModCount();
            if (cachedModCount == null || modCount == null) {
              throw new IllegalStateException(
                  "Missing " + Document.MOD_COUNT);
            }
            if (modCount.longValue() > cachedModCount.longValue()) {
              nodesCache.put(cacheKey, (NodeDocument) doc);
            }
          } else {
            nodesCache.put(cacheKey, (NodeDocument) doc);
          }
        }
        list.add(doc);
      }
      return list;
    } finally {
      lock.unlock();
      end("query", start);
    }
  }

  @Override
  public <T extends Document> void remove(Collection<T> collection, String key) {

  }

  @Override
  public <T extends Document> void remove(Collection<T> collection, List<String> keys) {

  }

  @Override
  public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps) {
    log("create", updateOps);
    List<T> docs = new ArrayList<T>();
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    XContentBuilder[] inserts = new XContentBuilder[updateOps.size()];

    String id = null;
    for (int i = 0; i < updateOps.size(); i++) {
      try {
        inserts[i] = jsonBuilder().startObject();

        UpdateOp update = updateOps.get(i);
        T target = collection.newDocument(this);
        UpdateUtils.applyChanges(target, update, comparator);
        docs.add(target);

        for (Map.Entry<UpdateOp.Key, UpdateOp.Operation> entry : update.getChanges().entrySet()) {
          UpdateOp.Key k = entry.getKey();
          UpdateOp.Operation op = entry.getValue();

          if (k.toString().equals(Document.ID)) {
            id = op.value.toString();
            inserts[i].field(ID, id);
          }

          switch (op.type) {
            case SET:
            case MAX:
            case INCREMENT: {
              inserts[i].field(k.toString(), op.value);
              break;
            }
            case SET_MAP_ENTRY: {
              Revision r = k.getRevision();
              if (r == null) {
                throw new IllegalStateException(
                    "SET_MAP_ENTRY must not have null revision");
              }
              inserts[i].field(k.getName(), new RevisionEntry(r, op.value));
              break;
            }
            case REMOVE_MAP_ENTRY:
              // nothing to do for new entries
              break;
            case CONTAINS_MAP_ENTRY:
              // no effect
              break;
          }
        }

        inserts[i].field(Document.MOD_COUNT, 1L);
        target.put(Document.MOD_COUNT, 1L);

        inserts[i].endObject();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      bulkRequest.add(client.prepareIndex(getIndexName(collection), getTypeName(collection), id)
          .setSource(inserts[i]));
    }
    long start = start();
    try {
      try {
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
          return false;
        }
        if (collection == Collection.NODES) {
          for (T doc : docs) {
            TreeLock lock = acquire(doc.getId());
            try {
              addToCache((NodeDocument) doc);
            } finally {
              lock.unlock();
            }
          }
        }
        return true;
      } catch (Exception e) {
        return false;
      }
    } finally {
      end("create", start);
    }
  }

  @Override
  public <T extends Document> void update(Collection<T> collection, List<String> keys, UpdateOp updateOp) {

  }

  @Override
  public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update) {
    log("createOrUpdate", update);
    T doc = findAndModify(collection, update, true, false);
    log("createOrUpdate returns ", doc);
    return doc;
  }

  @Override
  public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update) {
    log("findAndUpdate", update);
    T doc = findAndModify(collection, update, false, true);
    log("findAndUpdate returns ", doc);
    return doc;
  }

  @Override
  public void invalidateCache() {

  }

  @Override
  public <T extends Document> void invalidateCache(Collection<T> collection, String key) {
    if (collection == Collection.NODES) {
      TreeLock lock = acquire(key);
      try {
        nodesCache.invalidate(new StringValue(key));
      } finally {
        lock.unlock();
      }
    }
  }

  @Override
  public void dispose() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("ElasticSearch time: " + timeSum);
    }
    client.close();

    if (nodesCache instanceof Closeable) {
      try {
        ((Closeable) nodesCache).close();
      } catch (IOException e) {
        LOG.warn("Error occurred while closing Off Heap Cache", e);
      }
    }
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
