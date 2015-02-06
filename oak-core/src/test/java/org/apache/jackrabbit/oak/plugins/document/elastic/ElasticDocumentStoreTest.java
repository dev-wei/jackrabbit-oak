package org.apache.jackrabbit.oak.plugins.document.elastic;

import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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

    @Ignore
    @Test
    public void getVersionTest() throws Exception {
//    Object clusters = client
//        .admin()
//        .cluster()
//        .prepareNodesInfo()
//        .all()
//        .execute()
//        .actionGet();
    }

    @Ignore
    @Test
    public void get() throws Exception {
        Object movie = new GetRequestBuilder(client, "movies")
                .setPreference("_primary")
                .setId("1")
                .execute()
                .actionGet();
    }

    @Test
    public void search() throws Exception {
        try {
            SearchResponse response = client.prepareSearch("cluster_nodes")
                    .setSearchType(SearchType.DEFAULT)
                    .setQuery(QueryBuilders.filteredQuery(
                            QueryBuilders.matchAllQuery(),
                            new RangeFilterBuilder("id").gte("0").lte("a")
                    ))
                    .execute()
                    .actionGet();
            SearchHit[] hits = response.getHits().getHits();
        } catch (IndexMissingException e) {
            e.printStackTrace();
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    @Ignore
    @Test
    public void json() {
        try {
            JsopTokenizer t = new JsopTokenizer("{ \"r14b514b3233-0-1\" : \"c\",\"adsf\": { \"aaa\" : \"dsfdsf\"}}");
            t.read('{');
            JsonObject o = JsonObject.create(t);

            JsopBuilder w = new JsopBuilder();
            o.toJson(w);
            String a = w.toString();
        } catch (Exception exp) {
            exp.printStackTrace();
        }
    }
}
