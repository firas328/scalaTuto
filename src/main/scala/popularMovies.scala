import org.apache.spark._
import org.apache.log4j._
object popularMovies {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc=new SparkContext("local[*]","popularMovies")
    val lignes=sc.textFile("../scalaTuto/data/ml-100k/u.data")
    val movies=lignes.map(x=>(x.split("\t")(1).toInt,1))
    val flipedMovies =movies.reduceByKey((x,y)=>x+y)
    val sortedMovies=flipedMovies.map(x=>(x._2,x._1)).sortByKey().collect()
    sortedMovies.foreach(println)
    /*for (res<-sortedMovies){
      println(s"${res._2} , ${res._1}")
    }*/
  }

}
