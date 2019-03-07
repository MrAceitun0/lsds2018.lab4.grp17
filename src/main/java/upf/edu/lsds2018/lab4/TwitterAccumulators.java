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

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Twitter Accumulators");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        SparkContext sc = sparkContext.sc();
        
        // Create accumulators
        CollectionAccumulator<String> errorTweets = sc.collectionAccumulator("goodTweets");
        
        // Load input
        JavaRDD<String> stringRDD = sparkContext.textFile(inputDir).filter(x -> !x.isEmpty());
        //stringRDD.foreach(i -> System.out.println(i));
        
        JavaRDD<SimplifiedTweet> tweets = stringRDD.map(t ->{
        	Optional<SimplifiedTweet> st = SimplifiedTweet.fromJson(t);
        	
            if(st.equals(Optional.empty())){
            	errorTweets.add(t);
            	return null;
            }
            else {
            	return st.get();
            }
            
        }).filter(t -> t != null);
        
        System.out.println("# Total tweets: " + tweets.count()); 
        
        long parsingAttempts = tweets.count() + errorTweets.value().size();
        System.out.println("# Parsing attempts: " + parsingAttempts); 
        
        System.out.println("# Failed attempts: " + errorTweets.value().size()); 
        
        System.out.println("Erroring Tweets content:"); 
        
        for(int i = 0; i < 15; i++)//Print 15 error tweets content
        {
        	System.out.println(errorTweets.value().get(i));
        	System.out.println("\n");
        }
        
        sparkContext.close();
    }
}

