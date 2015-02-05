package org.apache.jackrabbit.oak.jcr;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.junit.Ignore;
import org.junit.Test;

import javax.jcr.*;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class ElasticSearchTest {

    @Test
    public void simple() throws RepositoryException {
        DocumentNodeStore ns = new DocumentMK.Builder()
                .setElastic()
                .getNodeStore();
        Repository repo = new Jcr(new Oak(ns)).createRepository();

//    Session session = repo.login(
//        new SimpleCredentials("admin", "admin".toCharArray())
//    );
//    Node root = session.getRootNode();
//    if (root.hasNode("hello")) {
//      Node hello = root.getNode("hello");
//      long count = hello.getProperty("count").getLong();
//      hello.setProperty("count", count + 1);
//      System.out.println("found the hello node, count=" + count);
//    } else {
//      System.out.println("creating the hello node");ls
//      root.addNode("hello").setProperty("count", 1);
//    }
//    session.save();
//    session.logout();
//    ns.dispose();
    }
}
