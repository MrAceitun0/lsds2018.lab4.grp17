package upf.edu.lsds2018.lab4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import upf.edu.lsds2018.lab4.model.SimplifiedTweet;

import java.util.Objects;


public class TwitterJoin {

    public static void main(String[] args) {
        String inputDir = args[0];
        String mapFile = args[1];
        String outputDir = args[2];

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Twitter Join");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Load input(s)
        JavaRDD<String> stringRDD = sparkContext.textFile(inputDir).filter(s -> !s.isEmpty());
        JavaRDD<String> mapLinesRDD = sparkContext.textFile(mapFile);

        final JavaPairRDD<String, String> mapISO639toLanguage = mapLinesRDD.mapToPair(s->{
            String[] lines = s.split("\t");
            if(!lines[1].isEmpty() && !lines[2].isEmpty()) return new Tuple2<>(lines[1],lines[2]);
            else return null;
        }).filter(Objects::nonNull);


        JavaRDD<SimplifiedTweet> tweets = stringRDD.map(s -> SimplifiedTweet.fromJson(s).orElse(null)).filter(Objects::nonNull);

        // Check it you can get the same output as the previous step
        final JavaPairRDD<Integer, String> countByLanguage = tweets.mapToPair(s -> new Tuple2<>(s.getLang(), 1))
                .reduceByKey((x,y)->x+y).leftOuterJoin(mapISO639toLanguage).mapToPair(t->{
                    if(t._2._2.isPresent()) {
                        return new Tuple2<>(t._2._2.get(), t._2._1);
                    }
                    else {
                        return new Tuple2<>("Undetermined", t._2._1);
                    }
                }).reduceByKey((x,y)->x+y).mapToPair(t->new Tuple2<Integer,String>(t._2,t._1));

        final JavaPairRDD<Integer, String> countByLanguageTop10 = sparkContext.parallelizePairs(countByLanguage.sortByKey(false).take(10));
        final JavaPairRDD<Integer, String> countByLanguageBottom10 = sparkContext.parallelizePairs(countByLanguage.sortByKey(true).take(10));

        long undetermined = countByLanguage.filter(t-> "Undetermined".equals(t._2)).first()._1;
        long allLanguages = countByLanguage.map(t->t._1).reduce((x,y)->x+y).longValue();
        double ratio = (1.0*undetermined) / (1.0*allLanguages);

        System.out.println("Ratio of undetermined tweets: " + ratio);

        // Save the map as RDD
        mapISO639toLanguage.saveAsTextFile(outputDir + "/map");
        countByLanguage.saveAsTextFile(outputDir + "/joined");
        countByLanguageTop10.saveAsTextFile(outputDir + "/top10");
        countByLanguageBottom10.saveAsTextFile(outputDir + "/bottom10");
    }

}



