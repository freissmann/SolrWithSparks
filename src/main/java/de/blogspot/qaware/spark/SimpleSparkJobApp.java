package de.blogspot.qaware.spark;

import de.blogspot.qaware.spark.common.ContextMaker;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Our very first Spark Job submitted!
 * <p>
 * Will do nothing more but count the elements in a list.
 */
public class SimpleSparkJobApp {

    public static void main(String[] args) throws Exception {

        SparkConf conf = ContextMaker.makeSparkConfig("Higher Math");

        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        List<String> calculationCounts = Arrays.asList("one", "two", "three");

        JavaRDD<String> logData = javaSparkContext.parallelize(calculationCounts);

        System.out.println("Counting to... " + logData.count());

        javaSparkContext.stop();
    }

}
