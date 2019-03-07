package upf.edu.lsds2018.lab4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import upf.edu.lsds2018.lab4.model.SimplifiedTweet;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Optional;
import java.util.Scanner;

public class TwitterBroadcastWithResource {

    public static void main(String[] args) throws IOException {
        String inputDir = args[0];
        String outputDir = args[1];

        HashMap<String, String> ccToCountry = readMapFromResource();

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Twitter Broadcast Variables With Resource");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Broadcast variable
        final Broadcast<HashMap<String, String>> bcastMap = sparkContext.broadcast(ccToCountry);

        // Load input
        JavaRDD<String> stringRDD = sparkContext.textFile(inputDir);
        JavaRDD<SimplifiedTweet> tweets = stringRDD.map(t -> SimplifiedTweet.fromJson(t).orElse(null)).filter(t -> t != null);

        // Retrieve and use the broadcasted map
        final JavaPairRDD<Integer, String> countByLanguage = tweets.mapToPair(t->{
        	HashMap<String, String> lMap = bcastMap.getValue();
        	return new Tuple2<String, Integer>(lMap.getOrDefault(t.getLang(), "Undetermined"), 1);
        }).reduceByKey((x, y)->x+y).mapToPair(p->new Tuple2<Integer, String>(p._2, p._1));

        // Report the following three outputs into a section of the README.md
        final JavaPairRDD<Integer, String> countByLanguageTop10 = sparkContext.parallelizePairs(countByLanguage.sortByKey(false).take(10));
        final JavaPairRDD<Integer, String> countByLanguageBottom10 = sparkContext.parallelizePairs(countByLanguage.sortByKey(true).take(10));
        
        long undetermined = countByLanguage.filter(t->"Undetermined".contentEquals(t._2)).first()._1;
        long languages = countByLanguage.map(t->t._1).reduce((x,y)->x+y).longValue();
        double r = (1.0 * undetermined) / (1.0 * languages);
        
        String ratio = "Ratio of undetermined/unknown tweets: " + r;        
        System.out.println(ratio);

        countByLanguage.saveAsTextFile(outputDir + "/all");
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
    private static HashMap<String, String> readMapFromResource() throws IOException 
    {
    	HashMap<String, String> lData = new HashMap<>();
        InputStream iStream = TwitterBroadcastWithResource.class.getClassLoader().getResourceAsStream("map.tsv");
        
        try
        {
        	Scanner scan;
        	scan = new Scanner(iStream);
        	
        	while(scan.hasNextLine())
        	{
        		String line = scan.nextLine();
        		String[] data = line.split("\t");
        		
        		if(!data[1].isEmpty())
        		{
        			lData.put(data[1], data[2]);
        		}
        	}
        	
        	scan.close();
        }
        catch(Exception e)
        {
        	e.printStackTrace();
        }
        
        return lData;
    }
}
