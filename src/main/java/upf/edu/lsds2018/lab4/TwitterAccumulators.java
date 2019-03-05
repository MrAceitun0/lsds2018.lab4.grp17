package upf.edu.lsds2018.lab4;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import upf.edu.lsds2018.lab4.model.SimplifiedTweet;

import java.util.Optional;

public class TwitterAccumulators {

    public static void main(String[] args) {
        String inputDir = args[0];
        String outputDir = args[1];

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Twitter Accumulators");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Create accumulators
        // TODO

        // Load input
        JavaRDD<String> stringRDD = sparkContext.textFile(inputDir);

        JavaRDD<SimplifiedTweet> tweets = // LOAD and USE ACCUMULATORS to provide the following printouts

        System.out.println("# Total tweets: "); // TODO
        System.out.println("# Parsing attempts: "); // TODO
        System.out.println("# Failed attempts: "); // TODO
        System.out.println("Erroring Tweets content:"); // TODO
    }
}

