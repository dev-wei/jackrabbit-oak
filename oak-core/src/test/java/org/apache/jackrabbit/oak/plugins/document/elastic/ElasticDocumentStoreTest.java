package org.apache.jackrabbit.oak.plugins.document.elastic;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ElasticDocumentStoreTest {
  private Client client;

  @Before
  public void prepareClient() {
    Node node = NodeBuilder.nodeBuilder().node();
    client = node.client();
  }

  @After
  public void closeClient() {
    client.close();
  }

  @Test
  public void getVersionTest() throws Exception {
    Object clusters = client
      .admin()
      .cluster()
      .prepareNodesInfo()
      .all()
      .execute()
      .actionGet();
  }

  @Test
  public void get() throws Exception{
    Object movie = new GetRequestBuilder(client, "movies")
      .setPreference("_primary")
      .setId("1")
      .execute()
      .actionGet();
  }
}
