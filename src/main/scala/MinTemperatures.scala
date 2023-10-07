import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math._

/** Find the minimum temperature by weather station */
object MinTemperatures {

  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
   Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc=new SparkContext("local[*]","MinTemoeratures")

    // Read each line of input data
    val lignes=sc.textFile("../scalaTuto/data/1800.csv")

    // Convert to (stationID, entryType, temperature) tuples
    val parsedLignes=lignes.map(parseLine)

    // Filter out all but TMIN entries
    val filtredLignes=parsedLignes.filter(x=>x._2=="TMAX")

    // Convert to (stationID, temperature)
    val stationTemps=filtredLignes.map(x=>(x._1,x._3))

    // Reduce by stationID retaining the minimum temperature found
    val reducedByStationid=stationTemps.reduceByKey((x,y)=>max(x,y))

    // Collect, format, and print the results
   val results=reducedByStationid.collect()

    for (result <- results.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station maximum temperature: $formattedTemp")
    }

  }
}
