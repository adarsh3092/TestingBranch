import java.io._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{explode,split,lit}
// Assignment solution it includes all questions solution.
object Assignment {
  //Question:1
  def saveintoFile(n:Int,path:String){
    var list=(1 to n)
    val data=list.map(x=>x*1.0).map(x=>x.toString())
       var pw=new PrintWriter(new File(path))
      data.foreach(x=> pw.append(x+" "))
 
    pw.close()
      }
  //Question:2
  def calculateAvgRDD(sc:SparkContext,path:String){
    val txtRDD=sc.textFile(path,8)
    val pairRDD=txtRDD.flatMap(x=>x.split(" ")).map(x=>(1,x.trim().toDouble))
    val result=pairRDD.reduce((x,y)=>(x._1+y._1,x._2+y._2))
    println(result._2/result._1)
                }
  //Question:3
  def calculateAvgDF(sqlContext:SQLContext,path:String){
    import sqlContext.implicits._
   val df= sqlContext.read.text(path)
   val numDF=df.withColumn("numbers",explode(split($"value"," ")))
                 .selectExpr("cast(numbers as int) numbers")
                   .select("numbers")
   val resultDF=numDF.withColumn("count", lit(1))
                                                 .groupBy("count")
                                                 .avg("numbers")
            resultDF.show()
   
        }
  def main(args:Array[String]){
    val path="/Users/adarsh/Desktop/myproject/adarsh"
    saveintoFile(50000, path)
    print("Sucessfully Insert!!")
    val conf=new SparkConf().setMaster("local[3]")
                            .setAppName("Assignment")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
   calculateAvgRDD(sc,path)
   calculateAvgDF(sqlContext,path)
   
  }
}