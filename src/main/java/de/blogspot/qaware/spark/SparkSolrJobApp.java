package de.blogspot.qaware.spark;

import com.lucidworks.spark.SolrRDD;
import de.blogspot.qaware.spark.common.ContextMaker;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;

public class SparkSolrJobApp {

    private static final String ZOOKEEPER_HOST_AND_PORT = "192.168.56.100:2181";
    private static final String SOLR_COLLECTION = "spark";
    private static final String QUERY_ALL = "*:*";

    public static void main(String[] args) throws Exception {
        String zkHost = ZOOKEEPER_HOST_AND_PORT;
        String collection = SOLR_COLLECTION;
        String queryStr = QUERY_ALL;

        JavaSparkContext javaSparkContext = ContextMaker.makeJavaSparkContext("Querying Solr");

        SolrRDD solrRDD = new SolrRDD(zkHost, collection);
        final SolrQuery solrQuery = SolrRDD.toQuery(queryStr);
        JavaRDD<SolrDocument> solrJavaRDD = solrRDD.query(javaSparkContext.sc(), solrQuery);

        JavaRDD<String> titleNumbers = solrJavaRDD.flatMap(doc -> {
            Object possibleTitle = doc.get("title");
            String title = possibleTitle != null ? possibleTitle.toString() : "";
            return Arrays.asList(title);
        }).filter(s -> !s.isEmpty());

        System.out.println("\n# of found titles: " + titleNumbers.count());

        // Now use schema information in Solr to build a queryable SchemaRDD
        SQLContext sqlContext = new SQLContext(javaSparkContext);

        // Pro Tip: SolrRDD will figure out the schema if you don't supply a list of field names in your query
        DataFrame tweets = solrRDD.asTempTable(sqlContext, queryStr, "documents");

        // SQL can be run over RDDs that have been registered as tables.
        DataFrame results = sqlContext.sql("SELECT * FROM documents where id LIKE 'one%'");

        // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        JavaRDD<Row> resultsRDD = results.javaRDD();

        System.out.println("\n\n# of documents where 'id' starts with 'one': " + resultsRDD.count());

        javaSparkContext.stop();
    }
}
