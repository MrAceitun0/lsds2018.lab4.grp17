package upf.edu.lsds2018.lab4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import upf.edu.lsds2018.lab4.model.SimpleTweetWithLang;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Optional;

public class TwitterBroadcastWithResource {

    public static void main(String[] args) throws IOException {
        String inputDir = args[0];
        String outputDir = args[1];

        HashMap<String, String> ccToCountry = readMapFromResource();

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Twitter Broadcast Variables With Resource");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Broadcast variable
        // TODO

        // Load input
        JavaRDD<String> stringRDD = sparkContext.textFile(inputDir);

        JavaRDD<SimplifiedTweet> tweets = // TODO (same as Lab3)

        // Retrieve and use the broadcasted map
        final JavaPairRDD<String, Integer> countByLanguage = // TODO

        // Report the following three outputs into a section of the README.md
        final JavaPairRDD<String, Integer> countByLanguageTop10 = // TODO
        final JavaPairRDD<String, Integer> countByLanguageBottom10 = // TODO
        System.out.println("Ratio of undetermined/unknown tweets: "); // TODO

        countByLanguage.saveAsTextFile(outputDir + "/all");
        countByLanguageTop10.saveAsTextFile(outputDir + "/top10");
        countByLanguageBottom10.saveAsTextFile(outputDir + "/bottom10");
    }

    /**
     * Read a static resource and return a mapping from 2-letter-code to English name.
     * Example:
     *
     *  "it" -> "Italian"
     *  "es" -> "Spanish; Castilian"
     *  etc...
     *
     * @return
     * @throws IOException
     */
    private static HashMap<String, String> readMapFromResource() throws IOException {
        // TODO
    }
}
