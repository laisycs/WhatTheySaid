package example;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.auth.Authorization;
import twitter4j.GeoLocation;
import twitter4j.auth.AccessToken;

import org.apache.spark.streaming.twitter.*;

public class TwitterStreamingAnalysis {
	private static final String consumerKey = "ntNC6bOzZ71az727bK18TpvmH";
	private static final String consumerSecret = "vWjc4arA0XHdyIl4Q6IBJadV34RK9IHujPNTq3NatGgtOq5vpj";
	private static final String accessToken = "2234236346-WFAszADHQd8BrqEJWXzJvhRebSaayGIM7M5V72A";
	private static final String accessTokenSecret = "wfxeD5GjjdK4LS9wdCOZwtmlhz1gWvzmVW8fsq2ZVQdJV";
	
	Set<String> posWords = new HashSet<String>();
	Set<String> negWords = new HashSet<String>();
	Set<String> stopWords = new HashSet<String>();
	
	private static transient JavaReceiverInputDStream<twitter4j.Status> receiver;
	
	public static void main(String args[]) throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TwitterStreammingExample");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
				
		Authorization oAuth = getTwitterOAuth();
		
		receiver = TwitterUtils.createStream(jssc, oAuth);
		
		
	}
	
	private static Authorization getTwitterOAuth() {
		ConfigurationBuilder  conf = new ConfigurationBuilder();
		conf.setDebugEnabled(true)
			.setOAuthConsumerKey(consumerKey)
			.setOAuthConsumerSecret(consumerSecret)
			.setOAuthAccessToken(accessToken)
			.setOAuthAccessTokenSecret(accessTokenSecret);
		
		OAuthAuthorization oAuth = new OAuthAuthorization(conf.build());
		oAuth.setOAuthConsumer(consumerKey, consumerSecret);
		oAuth.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret));
		
		return oAuth;
	}
	
	private void readWords() {
		
	}
}
