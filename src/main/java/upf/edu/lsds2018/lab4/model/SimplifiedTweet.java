package upf.edu.lsds2018.lab4.model;

import java.io.Serializable;
import java.util.Optional;

import com.google.gson.*;

public class SimplifiedTweet implements Serializable {

    private static JsonParser parser = new JsonParser();

    private final long tweetId;			// the id of the tweet ('id')
    private final String text;  		// the content of the tweet ('text')
    private final long userId;			// the user id ('user->id')
    private final String userName;		// the user name ('user'->'name')
    private final long followersCount;	// the number of followers ('user'->'followers_count')
    private final boolean isRetweeted;	// is it a retweet? (the object 'retweeted_status' exists?)
    private final Long retweetedUserId; // [if retweeted] ('retweeted_status'->'user'->'id')
    private final String retweetedUserName; // [if retweeted] ('retweeted_status'->'user'->'name')
    private final long timestampMs;		// seconds from epoch ('timestamp_ms')

    // ADDED FIELD FOR LAB 4
    private final String lang;  		// Language of the tweet as identified by Twitter ('lang')

    public SimplifiedTweet(long tweetId, String text, long userId, String userName,
                           long followersCount, boolean isRetweeted,
                           Long retweetedUserId, String retweetedUserName, long timestampMs, String lang) 
    {
    	this.tweetId = tweetId;        
        this.text = text;
        this.userId = userId;
        this.userName = userName;
        this.followersCount = followersCount;
        this.isRetweeted = isRetweeted;
        this.retweetedUserId = retweetedUserId;
        this.retweetedUserName = retweetedUserName;
        this.timestampMs = timestampMs;
        this.lang = lang;
    }

    /*Getters*/  
    public String getText() {
		return text;
	}
    
    public long getUserId() {
		return userId;
	}

	public long getTweetId() {
		return tweetId;
	}

	public String getUserName() {
		return userName;
	}

	public long getFollowersCount() {
		return followersCount;
	}

	public boolean isRetweeted() {
		return isRetweeted;
	}

	public Long getRetweetedUserId() {
		return retweetedUserId;
	}

	public String getRetweetedUserName() {
		return retweetedUserName;
	}

	public long getTimestampMs() {
		return timestampMs;
	}
	
	public String getLang()
	{
		return lang;
	}
	
    /**
     * Returns a {@link SimplifiedTweet} from a JSON String.
     * If parsing fails, for any reason, return an {@link Optional#empty()}
     *
     * @param jsonStr
     * @return an {@link Optional} of a {@link SimplifiedTweet}
     */
    public static Optional<SimplifiedTweet> fromJson(String jsonStr) {
    	try {
    		JsonElement je = parser.parse(jsonStr);
            JsonObject jo = je.getAsJsonObject();

            // initialize variables
            long tweetId = 0L;
            String text = null;
            long userId = 0L;
            String userName = null;
            long followersCount = 0L;
            long timestampMs = 0L;
            boolean isRetweeted = false;
            Long retweetedUserId = 0L;
            String retweetedUserName = null;
            String lang = null;

            if (jo.has("id")) {
                tweetId = jo.get("id").getAsLong();
            } else {
                return Optional.empty();
            }

            if (jo.has("text")) {
                text = jo.get("text").getAsString();
                //System.out.println(text);
            }

            if (jo.has("user")) {
                JsonObject userObj = jo.get("user").getAsJsonObject(); // cast method
                if (userObj.has("id")) {
                    userId = userObj.get("id").getAsLong();
                } else {
                    return Optional.empty();
                }
                if (userObj.has("name")) {
                    userName = userObj.get("name").getAsString();
                }
                if (userObj.has("followers_count")) {
                    followersCount = userObj.get("followers_count").getAsLong();
                } else {
                    return Optional.empty();
                }
            } else {
                return Optional.empty();
            }
            
            if (jo.has("timestamp_ms")) {
                timestampMs = jo.get("timestamp_ms").getAsLong();
            } else {
                return Optional.empty();
            }

            if (jo.has("retweeted_status")) {
                isRetweeted = true;
                JsonObject retweetObj = jo.get("retweeted_status").getAsJsonObject();
                if (retweetObj.has("user")) {
                    JsonObject retweetUserObj = retweetObj.get("user").getAsJsonObject();
                    if (retweetUserObj.has("id")) {
                    	if(!retweetUserObj.get("id").isJsonNull())
                    	{
                    		retweetedUserId = retweetUserObj.get("id").getAsLong();
                    	}      
                    }
                    else {
                    	return Optional.empty();
                    }
                                    
                    if (retweetUserObj.has("name")) {
                        retweetedUserName = retweetUserObj.get("name").getAsString();
                    } 
                } else {
                    return Optional.empty();
                }
            }
            
            if (jo.has("lang")) {
                lang = jo.get("lang").getAsString();
            }
            
            return Optional.of(new SimplifiedTweet(tweetId, text, userId, userName, followersCount, isRetweeted,
                    retweetedUserId, retweetedUserName, timestampMs, lang));
    	}
    	catch (Exception e){
    		e.printStackTrace();
    		return null;
    	}
        
    }

    @Override
    public String toString() {
    	String st = "Tweet ID: " + getTweetId() + "\nUser ID: "+ getUserId() + "\nUser Name: "+ getUserName() + "\nText: "+getText() + 
        		"\nFollowers: "+getFollowersCount() + "\nIs Retweeted: " + isRetweeted() + "\nRetweeted User ID: " + getRetweetedUserId() + "\nRetweeted User Name: " + 
        		getRetweetedUserName() + "\nTime Stamp (ms): " + getTimestampMs() + "\nLanguage: "+ getLang();
    	
    	return st;
    }
}

