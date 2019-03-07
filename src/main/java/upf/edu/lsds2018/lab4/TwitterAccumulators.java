package upf.edu.lsds2018.lab4;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import upf.edu.lsds2018.lab4.model.SimplifiedTweet;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class TwitterAccumulators {

    private static Long checkErrors(Long numErrors) {
        return numErrors>15 ? 15 : numErrors;
    }
    public static void main(String[] args) throws IOException {
        String inputDir = args[0];
        
        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Twitter Accumulators");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        SparkContext sc = sparkContext.sc();
        
        // Create accumulators
        CollectionAccumulator<String> errorTweets = sc.collectionAccumulator("goodTweets");
        LongAccumulator countErrors = sc.longAccumulator("countErrors");
        
        // Load input
        JavaRDD<String> stringRDD = sparkContext.textFile(inputDir).filter(x -> !x.isEmpty());
        //stringRDD.foreach(i -> System.out.println(i));
        
        JavaRDD<SimplifiedTweet> tweets = stringRDD.map(t ->{
        	Optional<SimplifiedTweet> st = SimplifiedTweet.fromJson(t);
        	
            if(st.equals(Optional.empty())){
            	errorTweets.add(t);
            	countErrors.add(1);
            	return null;
            }
            else {
            	return st.get();
            }
            
        }).filter(t -> t != null);
        
        System.out.println("# Total tweets: " + tweets.count()); 
        
        Long countErrorsValue = countErrors.value();
        System.out.println("# Parsing attempts: " + (tweets.count() + countErrorsValue)); 
        
        System.out.println("# Failed attempts: " + countErrorsValue); 
        
        System.out.println("Erroring Tweets content:"); 
        
        List<String> errorTweetsString = errorTweets.value();


        for(int i = 0; i < checkErrors(countErrorsValue); i++)//Print 15 error tweets content
        {
        	System.out.println(errorTweetsString.get(i));
        	System.out.println("\n");
        }
        
        BufferedWriter writer = new BufferedWriter(new FileWriter("../outAccumulators/info.txt"));
        try {
			writer.write("# Total tweets: " + tweets.count()+"\n");
			writer.write("# Parsing attempts: " + (tweets.count() + countErrorsValue)+"\n");
			writer.write("# Failed attempts: " + countErrorsValue+"\n");
			writer.write("Erroring Tweets content:\n");

			for(int i = 0; i < checkErrors(countErrorsValue); i++)//Print 15 error tweets content
	        {
				writer.write(errorTweetsString.get(i)+"\n");
	        }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
         
        writer.close();
        
        sparkContext.close();
    }
}

