package upf.edu.lsds2018.lab4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import upf.edu.lsds2018.lab4.model.SimpleTweetWithLang;

import java.io.IOException;
import java.util.Optional;

public class TwitterJoin {

    public static void main(String[] args) throws IOException {
        String inputDir = args[0];  // An S3 input
        String mapFile = args[1];   // An S3 file
        String outputDir = args[2];

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Twitter Join");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Load input(s)
        JavaRDD<String> stringRDD = sparkContext.textFile(inputDir);
        JavaRDD<String> mapLinesRDD = sparkContext.textFile(mapFile);

        final JavaPairRDD<String, String> mapISO639toLanguage = // TODO

        JavaRDD<SimplifiedTweet> tweets = // TODO. Same as Lab3

        // Check it you can get the same output as the previous step
        final JavaPairRDD<String, Integer> countByLanguage = // TODO
        final JavaPairRDD<String, Integer> countByLanguageTop10 = // TODO
        final JavaPairRDD<String, Integer> countByLanguageBottom10 = // TODO
        System.out.println("Ratio of undetermined/unknown tweets: "); // TODO

        // Save the map as RDD
        mapISO639toLanguage.saveAsTextFile(outputDir + "/map");
        countByLanguage.saveAsTextFile(outputDir + "/joined");
        countByLanguageTop10.saveAsTextFile(outputDir + "/top10");
        countByLanguageBottom10.saveAsTextFile(outputDir + "/bottom10");
    }

}

