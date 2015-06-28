package de.blogspot.qaware.solr;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class SimpleShardingApp {

    /**
     * Our ZooKeepers IP-address and port.
     */
    private static final String ZOOKEEPER_URL = "192.168.56.100:2181";

    /**
     * The name of our Solr-Collection.
     */
    private static final String COLLECTION_NAME = "spark";

    /**
     * Number of documents which will be added to the first shard.
     */
    private static final int SHARD_ONE_NR_DOCUMENTS = 11_111;

    /**
     * Number of documents which will be added to the second shard.
     */
    private static final int SHARD_TWO_NR_DOCUMENTS = 55_555;

    /**
     * First shard's prefix.
     */
    private static final String SHARD_ONE_PREFIX = "one";

    /**
     * Second shard's prefix.
     */
    private static final String SHARD_TWO_PREFIX = "two";

    public static void main(String[] args) throws IOException, SolrServerException {

        try (CloudSolrClient solrClient = new CloudSolrClient(ZOOKEEPER_URL)) {
            solrClient.setDefaultCollection(COLLECTION_NAME);

            // Uncomment the following line if you wish to clean the collection before!
            solrClient.deleteByQuery("*:*");

            // Adds the Documents to Solr.
            solrClient.add(createSolrDocuments());

            // Make Solr write the changes.
            solrClient.commit();
        }
    }

    private static Collection<SolrInputDocument> createSolrDocuments() {
        Collection<SolrInputDocument> docsToAdd = new ArrayList<>();

        // Create some documents for shard 1
        docsToAdd.addAll(createDocuments(SHARD_ONE_NR_DOCUMENTS, SHARD_ONE_PREFIX));

        // And some more for shard 2
        docsToAdd.addAll(createDocuments(SHARD_TWO_NR_DOCUMENTS, SHARD_TWO_PREFIX));

        return docsToAdd;
    }

    private static Collection<SolrInputDocument> createDocuments(int nrDocuments, String shardPrefix) {
        Collection<SolrInputDocument> documents = new ArrayList<>();

        for (int i = 0; i < nrDocuments; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", shardPrefix + "!" + i);
            doc.addField("title", "Document Number: " + i);
            documents.add(doc);
        }

        return documents;
    }
}
