import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder


object StreamingTask {
  def main(args:Array[String]){
    /*val conf=new SparkConf().setAppName("My local apis")
                            .setMaster("local[3]")*/
   //val sc=new SparkContext(conf)
  val ssc=new StreamingContext("local[6]","my app",Seconds(20))
  val topic=Set("test")
  val boker="localhost:9092"
  
  val kafkaParams=Map("metadata.broker.list"->boker)
  val lines=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topic)
  val words=lines.window(Seconds(4000), Seconds(80)).map(_._2)
  words.print()
  words.map(x=>x.split(",")).filter(x=>x.length>0).map(x=>(x(6),x(0),x(1),x(3))).print()//.toDF
  ssc.checkpoint("/Users/adarsh/Desktop/myproject/checkpoints")
   ssc.start()
   ssc.awaitTermination()
  }
}