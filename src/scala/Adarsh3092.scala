import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.Seconds


object Adarsh3092 {
  def main(args:Array[String]){
    val consumerKey="pqVAUZejtktBZr7bMS6a8GphQ"
 val consumerSecret ="DFymTBfxZ1hwifTFVuYuGdzTB6YEmmZbxeg9JjbB4CjbeSzFUk"
 val accessToken ="934193876-w95BwNFEMTj91eUEYTnn2q9S4dzGxiLmxWDwpUbd"
 val accessTokenSecret ="HyeqeD2l6bG4nxXhdgnSPgPbzxHjV9pEBs3EGElzWYPWU"
  val appName = "Adarsh3092"    
    val conf = new SparkConf()    
    conf.setAppName(appName).setMaster("local[3]")    
    val ssc = new StreamingContext(conf, Seconds(15))   
    
    
   // val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)    
    //val filters = args.takeRight(args.length - 4)    
    val cb = new ConfigurationBuilder    
    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)    
      .setOAuthConsumerSecret(consumerSecret)    
      .setOAuthAccessToken(accessToken)    
      .setOAuthAccessTokenSecret(accessTokenSecret)    
    val auth = new OAuthAuthorization(cb.build)    
    val tweets = TwitterUtils.createStream(ssc, Some(auth))    
    val englishTweets = tweets.filter(_.getLang() == "en") 
      englishTweets.print()
    ssc.start()    
    ssc.awaitTermination()    
  }
}