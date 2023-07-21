package Pack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object SparkClsExp {
  def main(args:Array[String]) = {
    
    val sc = new SparkContext(master ="local[*]",appName ="sk_Eg")
    sc.setLogLevel("Error")
    
     val rdd = sc.textFile("file:///C:/Users/Satish konatham/Documents/txs.txt")
     rdd.take(10).foreach(println)
     val rdd1 = rdd.map(x=> x.split(","))
     
     val ObjEtl =new EtlPipeLineImp
     ObjEtl.ExcuteEtl(rdd1)
  }
  
  class EtlPipeLineImp()
  {
    def Extract(rdd:RDD[Array[String]]) 
  {
      println("-------------------------------------")
      println("Entering into Extract Method")
     val rdd2 = rdd.filter(x=> x(2) == "Yangon")
     val rdd3 = rdd2.map(x=> x(7).toFloat)
     val totalsales = rdd3.sum()
     println(s"Total price of sale in yangon is : $totalsales" )
  }
  
  def Transform(rdd:RDD[Array[String]])
  {
    println("--------------------------------------------")
    println("Entering into Transform Method")
       val rdd2 = rdd.filter(x=> x(2).contains("Yangon"))
        val rdd3 = rdd2.map(x=> (x(5),1))
        val rdd4 = rdd3.reduceByKey((x,y) => x+y)
        val rdd5 = rdd4.sortBy(x => x._2,false,1)
        rdd5.foreach(println)
  }
  
  def Load(rdd:RDD[Array[String]])
  {
    println("----------------------------------------------")
    println("Entering into Load Method")
    val rdd2 = rdd.filter(x=> x(2).contains("Mandalay"))
    val rdd3 =rdd2.map(x=>(x(2),(x(9).toFloat,1)))
    val rdd4 = rdd3.reduceByKey((x,y) =>(x._1+y._1,x._2+y._2))
    rdd4.foreach(println)
  }
  
   def ExcuteEtl(rdd:RDD[Array[String]])
   {
     println("Exceuting the pipeLine")
     Extract(rdd)
     Transform(rdd)
     Load(rdd)
   }
   
  }
  
}