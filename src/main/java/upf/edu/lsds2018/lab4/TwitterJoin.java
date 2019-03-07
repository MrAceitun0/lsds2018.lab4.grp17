package upf.edu.lsds2018.lab4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import upf.edu.lsds2018.lab4.model.SimplifiedTweet;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
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
            if(!lines[1].isEmpty() && !lines[2].isEmpty()) return new Tuple2<String, String>(lines[1],lines[2]);
            else return null;
        }).filter(t->t!=null);

        JavaRDD<SimplifiedTweet> tweets = stringRDD.map(s -> SimplifiedTweet.fromJson(s).orElse(null)).filter(s->s!=null);

        // Check it you can get the same output as the previous step
        final JavaPairRDD<Integer, String> countByLanguage = tweets.mapToPair(s -> new Tuple2<String, Integer>(s.getLang(), 1))
                .reduceByKey((x,y)->x+y).leftOuterJoin(mapISO639toLanguage).mapToPair(s->{
                    if(s._2._2.isPresent()) return new Tuple2<String, Integer>(s._2._2.get(), s._2._1);
                    else return new Tuple2<String, Integer>("Undetermined", s._2._1);
                }).reduceByKey((x,y)->x+y).mapToPair(s->new Tuple2<Integer, String>(s._2 , s._1));

        final JavaPairRDD<Integer, String> countByLanguageTop10 = sparkContext.parallelizePairs(countByLanguage.sortByKey(false).take(10));
        final JavaPairRDD<Integer, String> countByLanguageBottom10 = sparkContext.parallelizePairs(countByLanguage.sortByKey(true).take(10));

        long undetermined = countByLanguage.filter(s-> "Undetermined".equals(s._2)).first()._1;
        long allLanguages = countByLanguage.map(s->s._1).reduce((x,y)->x+y).longValue();
        double r = (1.0 * undetermined) / (1.0 * allLanguages);

        String ratio = "Ratio of undetermined/unknown tweets: " + r;        
        System.out.println(ratio);
        
        // Save the map as RDD
        mapISO639toLanguage.saveAsTextFile(outputDir + "/map");
        countByLanguage.saveAsTextFile(outputDir + "/joined");
        countByLanguageTop10.saveAsTextFile(outputDir + "/top10");
        countByLanguageBottom10.saveAsTextFile(outputDir + "/bottom10");
        
        try 
        {
        	BufferedWriter writer = new BufferedWriter(new FileWriter(outputDir + "/ratio.txt"));
			writer.write(ratio);
			writer.close();
		} 
        catch (IOException e) 
        {
			e.printStackTrace();
		}
        
        sparkContext.close();
    }

}



