package de.qaware.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

final class ContextMaker {

    /**
     * The Spark-Master URL.
     */
    private static final String SPARK_MASTER_URL = "spark://192.168.56.100:7077";

    /**
     * The IP to bind the driver to (in our case the VirtualBox host only adapter's IP)
     */
    private static final String LOCAL_HOST_ONLY_IP = "192.168.56.1";

    /**
     * The folder where we all Jars are stored which will be added to the JavaSparkContext.
     */
    private static final String LIBRARIES_FOLDER = "libs_to_transfer";

    /**
     * Creates a configured JavaSparkContext.
     *
     * @param appName the name used for this Task.
     * @return a JavaSparkContext ready to go.
     */
    public static JavaSparkContext make(String appName) {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster(SPARK_MASTER_URL)
                .set("spark.driver.host", LOCAL_HOST_ONLY_IP);

        JavaSparkContext jsc = new JavaSparkContext(conf);

        // This project's Jar-File, as we use Lambdas for our calculation
        jsc.addJar(Paths.get("target", "spark-using-solr-blog-1.0-SNAPSHOT.jar").toString());

        // Add all jars from the Library Folder to the JavaSparkContext
        getAllLibrariesFrom(LIBRARIES_FOLDER)
                .forEach(jsc::addJar);

        return jsc;
    }

    /**
     * Returns all Jars from a given folder.
     *
     * @param librariesFolder the folder to get the jars from.
     * @return a Iterable of path-Strings.
     */
    private static Iterable<String> getAllLibrariesFrom(String librariesFolder) {
        try {
            return Files.list(Paths.get(librariesFolder))
                    .filter(Files::isRegularFile)
                    .map(Object::toString)
                    .filter(s -> s.endsWith(".jar"))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(String.format("An error occurred while reading Jars from %s.", librariesFolder), e);
        }
    }

}
