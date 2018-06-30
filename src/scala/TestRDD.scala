import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter.TwitterUtils


object TestRDD {
  def main(args:Array[String]){
   /* val consumerKey="pqVAUZejtktBZr7bMS6a8GphQ"
    val consumerSecret="DFymTBfxZ1hwifTFVuYuGdzTB6YEmmZbxeg9JjbB4CjbeSzFUk"
    val accessToken="934193876-abw3E2R0OXMoo89shA3Yp3bh1UiPmXnSYwiUkXf6"
    val accessTokenSecret="	BkhOI8m3WZjwAloskFdbiAulThggmeiG8Q1futXuVSEap"
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    val ssc=new StreamingContext(sc,Seconds(5))
    val tweets=TwitterUtils.createStream(ss
    tweets.map(x=>x.getText).print()
    */
    val conf=new SparkConf().setAppName("local spark").setMaster("local[2]")
    val sc =new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    //val list=Seq((1,"adarsh"),(2,"amana"),(3,"priyanka"),(2,"abha"))
   // println(list)
    import sqlContext.implicits._
 //  val rdd= sc.parallelize(list,3).toDF("emp","name")
   val csvDF=sqlContext.read.option("header", true).csv("/Users/adarsh/Desktop/myproject/Movie_Id_Titles")
     csvDF.show()
   
   
  }
}