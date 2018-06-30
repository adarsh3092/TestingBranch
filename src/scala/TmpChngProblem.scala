import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


object TmpChngProblem {
  val tmpDiff=()
  def main(args:Array[String]){
    val conf =new SparkConf().setAppName("Bulidng tmp chnahing")
                              .setMaster("local[4]")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    val tmp=sqlContext.read
                  .option("header",true)
                  
                  .csv("/Users/adarsh/Desktop/myproject/ml-20m/HVAC.csv")
   
    tmp.show()
  }
}