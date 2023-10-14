package sparkSql


import org.apache.log4j._
import org.apache.spark.sql.SparkSession
object sparkSQL {
  case class Person(ID:Int,name:String,age:Int,numFreinds:Int)
  def mapper(line: String): Person = {
    val fields = line.split(',')

    val person: Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    val lignes=spark.sparkContext.textFile("../scalaTuto/data/fakefriends.csv")
    val people=lignes.map(mapper)
    import spark.implicits._
    val schemaPeople=people.toDS()
    schemaPeople.printSchema()
    schemaPeople.createOrReplaceTempView("people")
    val teenagers=spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    val results=teenagers.collect()
    results.foreach(println)

  }
}
