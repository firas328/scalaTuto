import org.apache.spark._
import org.apache.log4j._

import java.nio.charset.CodingErrorAction
import scala.io.{Codec, Source}
object popularMoviesBetter {
 def loadMoviesNames() : Map[Int,String]={
   //Handle character encoding issues
   implicit val codec=Codec("UTF-8")
   codec.onMalformedInput(CodingErrorAction.REPLACE)
   codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
   var moviesName:Map[Int,String]=Map()
   val lignes=Source.fromFile("../scalaTuto/data/ml-100k/u.item").getLines()
   for (ligne<-lignes){
     val fields=ligne.split('|')
     if(fields.length>1){
       moviesName += (fields(0).toInt-> fields(1))
     }
   }
   return moviesName
 }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc=new SparkContext("local[*]","SortedMoviesBetter")
    val lignes = sc.textFile("../scalaTuto/data/ml-100k/u.data")
    val namesDict=sc.broadcast(loadMoviesNames)
    val movies = lignes.map(x => (x.split("\t")(1).toInt, 1))
    val flipedMovies = movies.reduceByKey((x, y) => x + y)
    val sortedMovies = flipedMovies.map(x => (x._2, x._1))
    val nSortedMovies=sortedMovies.map(x=>(x._1,namesDict.value(x._2))).sortByKey().collect()
    nSortedMovies.foreach(println)
  }
}
