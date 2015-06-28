package de.qaware.spark;

import com.lucidworks.spark.SolrRDD;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Collections;

public class SparkSolrJobApp {

    public static void main(String[] args) throws Exception {
        String zkHost = "192.168.56.100:2181";
        String collection = "spark";
        String queryStr = "*:*";

        JavaSparkContext javaSparkContext = ContextMaker.make("Querying Solr");

        SolrRDD solrRDD = new SolrRDD(zkHost, collection);
        final SolrQuery solrQuery = SolrRDD.toQuery(queryStr);
        JavaRDD<SolrDocument> solrJavaRDD = solrRDD.query(javaSparkContext.sc(), solrQuery);

        JavaRDD<String> titleNumbers = solrJavaRDD.flatMap(new FlatMapFunction<SolrDocument, String>() {
            public Iterable<String> call(SolrDocument doc) {
                Object possibleTitle = doc.get("title");
                String title = possibleTitle != null ? possibleTitle.toString() : "";
                return Collections.singletonList(title);
            }
        });
        System.out.println("\n# of found titles: " + titleNumbers.count());

        JavaRDD<String> filteredDocuments = titleNumbers.filter(new Function<String, Boolean>() {
            public Boolean call(String title) throws Exception {
                int number = Integer.parseInt(title.replace("Document Number: ", ""));
                return number % 1001 == 0;
            }
        });

        System.out.println("\n# of titles containing a number modulo 1001: " + filteredDocuments.count());

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
