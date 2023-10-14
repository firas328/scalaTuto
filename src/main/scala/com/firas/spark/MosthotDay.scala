package com.firas.spark

import org.apache.log4j._
import org.apache.spark._

object MosthotDay{
  def sparseFile(ligne:String) = {
    val parsLigne=ligne.split(",")
    val day=parsLigne(1)
    val entryType=parsLigne(2)
    val temp=parsLigne(3).toFloat
    (day,entryType,temp)
  }
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MostHotDay")
    val file = sc.textFile("../scalaTuto/data/1800.csv")
    val lignes = file.map(sparseFile)
    val dayWithTempMax = lignes.filter(x => x._2 == "TMAX")
    val dayTemp = dayWithTempMax.map(x => (x._1, x._3))
    val Max = dayTemp.reduce((x, y) => if (x._2 > y._2) x else y)
    println(s"${Max._1} maximum temperature: ${Max._2}")
  }
}
