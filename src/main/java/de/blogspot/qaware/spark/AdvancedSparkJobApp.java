package de.blogspot.qaware.spark;

import de.blogspot.qaware.spark.common.ContextMaker;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.LongStream;

public class AdvancedSparkJobApp {

    public static void main(String[] args) throws Exception {
        JavaSparkContext javaSparkContext = ContextMaker.makeJavaSparkContext("Counting Factorials");

        List<Integer> calculationCounts = Arrays.asList(5000, 1000, 5000, 2000);

        JavaRDD<Integer> logData = javaSparkContext.parallelize(calculationCounts);

        logData.foreach(nrOfCalculations -> {
            LongStream.rangeClosed(1, nrOfCalculations)
                    .mapToObj(BigInteger::valueOf)
                    .forEach(AdvancedSparkJobApp::calculateFactorial);
        });

        System.out.printf("Calculated '%s' factorials %n", calculationCounts.stream().reduce(0, (x, y) -> x + y));

        javaSparkContext.stop();
    }

    private static BigInteger calculateFactorial(BigInteger n) {
        return LongStream.rangeClosed(2, n.longValue())
                //.parallel() // can be used to increase performance
                .mapToObj(BigInteger::valueOf)
                .reduce(BigInteger.valueOf(1), BigInteger::multiply);
    }
}
