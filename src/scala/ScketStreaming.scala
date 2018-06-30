import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


object ScketStreaming {
  def main(args:Array[String]){
    val conf=new SparkConf().setAppName("Context of Stream").setMaster("local[2]")
    val ssc=new StreamingContext(conf,Seconds(2))
    val lines=ssc.socketTextStream("localhost", 9999)
    val words= lines.flatMap(_.split(" "))
    val pairs=words.map(x=>(x,1))
       pairs.reduceByKey(_+_).print()
       ssc.start()
       ssc.awaitTermination()
  }
}