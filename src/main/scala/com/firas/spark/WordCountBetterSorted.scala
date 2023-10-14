package com.firas.spark

import org.apache.log4j._
import org.apache.spark._

object WordCountBetterSorted {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "CountBetter")
    val t = sc.textFile("../scalaTuto/data/book.txt")
    val text = t.map(x => x.toLowerCase)
    val countWorld = text.flatMap(x => x.split("\\W+")).map(x => (x, 1)).reduceByKey((x, y) => x + y)
    val sorted = countWorld.map(x => (x._2, x._1)).sortByKey().collect()
    for (res <- sorted) {
      println(s"${res._2} , ${res._1}")
    }

  }


}
