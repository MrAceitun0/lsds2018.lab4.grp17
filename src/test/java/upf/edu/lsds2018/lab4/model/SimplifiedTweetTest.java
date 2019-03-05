package upf.edu.lsds2018.lab4.model;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.Serializable;
import java.util.Optional;

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Optional;
import java.util.Scanner;

import static org.junit.Assert.*;

import upf.edu.lsds2018.lab4.model.SimplifiedTweet;

public class SimplifiedTweetTest {
	@Test
    public void realTweetParse()
    {
    	File file = new File("res/test1.json");
    	SimplifiedTweet sTweet;
    	Long id = 108312062L;
    	
		try 
		{
			Scanner scan;
			scan = new Scanner(file);
			
			String line = scan.nextLine();
			
	    	Optional<SimplifiedTweet> st = SimplifiedTweet.fromJson(line);
	    	
	    	sTweet = st.get();
	    	
	    	assertEquals("Tweet is not as expected", sTweet.getText(), "RT @Philip_Lawrence: Well done Israel. Let the record state I called this weeks ago. #EUROVISION");
	    	assertEquals("ID is not as expected", sTweet.getUserId(), 47917269L);
	    	assertEquals("Tweet ID is not as expected", sTweet.getTweetId(), 995438237975007233L);
	    	assertEquals("User name is not as expected", sTweet.getUserName(), "Sarit Wilson Chen");
	    	assertEquals("Followers count is not as expected", sTweet.getFollowersCount(), 243);
	    	assertEquals("Is retweeted is not as expected", sTweet.isRetweeted(), true);
	    	assertEquals("Retweeted user ID is not as expected", sTweet.getRetweetedUserId(), id);
	    	assertEquals("Retweeted user name is not as expected", sTweet.getRetweetedUserName(), "Philip Lawrence");
	    	assertEquals("Time stamp is not as expected", sTweet.getTimestampMs(), 1526165944938L);
	    	assertEquals("Language is not as expected", sTweet.getLang(), "en");
	    	
	    	scan.close();
	    	//assertEquals(" is not as expected", sTweet.get(), );
		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}    	
    }
    
	@Test
    public void invalidJsonParse()
    {
    	File file = new File("res/test2.json");
    	//SimplifiedTweet sTweet;
    	
		try 
		{
			Scanner scan;
			scan = new Scanner(file);
			
			String line = scan.nextLine();
	    	
	    	Optional<SimplifiedTweet> st = SimplifiedTweet.fromJson(line);
	    	
	    	assertEquals("Return of invalid JSON is not as expected", st.isPresent(), false);
	    	
	    	scan.close();
		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	
    @Test
    public void validJsonParse()
    {
    	File file = new File("res/test3.json");
    	SimplifiedTweet sTweet;
    	Long id = 108312062L;
    	
		try 
		{
			Scanner scan;
			scan = new Scanner(file);
			
			String line = scan.nextLine();
	    	
	    	Optional<SimplifiedTweet> st = SimplifiedTweet.fromJson(line);
	    	System.out.println(st.toString());
	    	sTweet = st.get();
	    	
	    	assertEquals("Tweet is not as expected", sTweet.getText(), null);
	    	assertEquals("ID is not as expected", sTweet.getUserId(), 47917269L);
	    	assertEquals("Tweet ID is not as expected", sTweet.getTweetId(), 995438237975007233L);
	    	assertEquals("User name is not as expected", sTweet.getUserName(), "Sarit Wilson Chen");
	    	assertEquals("Followers count is not as expected", sTweet.getFollowersCount(), 243);
	    	assertEquals("Is retweeted is not as expected", sTweet.isRetweeted(), true);
	    	assertEquals("Retweeted user ID is not as expected", sTweet.getRetweetedUserId(), id);
	    	assertEquals("Retweeted user name is not as expected", sTweet.getRetweetedUserName(), "Philip Lawrence");
	    	assertEquals("Timestamp is not as expected", sTweet.getTimestampMs(), 1526165944938L);
	    	assertEquals("Language is not as expected", sTweet.getLang(), null);
	    	
	    	scan.close();
		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}