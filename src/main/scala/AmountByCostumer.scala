
import org.apache.log4j._
import org.apache.spark.SparkContext

object AmountByCostumer {
     def parsefile(record:String)={
       val fields=record.split(",")
       val costumerId=fields(0).toInt
       val amount=fields(2).toFloat
       (costumerId,amount)
     }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc=new SparkContext("local[*]","AmountByCostumer")
    val File=sc.textFile("../scalaTuto/data/customer-orders.csv")
    val parsedFile=File.map(parsefile)
    val totalByCost=parsedFile.reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1)).sortByKey().collect()
    for (record<-totalByCost){
      println(s"${record._2} , ${record._1}")
    }
  }
}
