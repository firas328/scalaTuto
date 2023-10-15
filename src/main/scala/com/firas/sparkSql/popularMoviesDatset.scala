package com.firas.sparkSql
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

import java.nio.charset.CodingErrorAction
import scala.io.{Codec, Source}
object popularMoviesDatset {
  def loadMoviesNames(): Map[Int, String] = {
    //Handle character encoding issues
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    var moviesName: Map[Int, String] = Map()
    val lignes = Source.fromFile("../scalaTuto/data/ml-100k/u.item").getLines()
    for (ligne <- lignes) {
      val fields = ligne.split('|')
      if (fields.length > 1) {
        moviesName += (fields(0).toInt -> fields(1))
      }
    }
    return moviesName
  }
final case class movie(id:Int)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    val lignes=spark.sparkContext.textFile("../scalaTuto/data/ml-100k/u.data").map(x=>movie(x.split("\t")(1).toInt))
    import spark.implicits._
    val moviesDS=lignes.toDS()
    val topPopular=moviesDS.groupBy("id").count().orderBy(desc("count")).cache()
    topPopular.show()
    val top10=topPopular.take(10)
    val names=loadMoviesNames()
    for (res<-top10){
      println(s"${names(res(0).asInstanceOf[Int])} : ${res(1)}")

    }
    spark.stop()
  }
}
