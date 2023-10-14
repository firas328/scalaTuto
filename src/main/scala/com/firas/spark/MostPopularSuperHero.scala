package com.firas.spark

import org.apache.log4j._
import org.apache.spark._
object MostPopularSuperHero {
  def parseNames(ligne :String):Option[(Int,String)]={
    val fields =ligne.split('\"')
    if (fields.length>1){
      Some(fields(0).trim().toInt,fields(1))
    }else None
  }
  def countCoConcurrence(ligne:String):(Int,Int)={
    val elments=ligne.split("\\s+")
    (elments(0).toInt,elments.length-1)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc=new SparkContext("local[*]","MostPopularSuperHero")
    val namesFile=sc.textFile("../scalaTuto/data/Marvel-names.txt")
    val names=namesFile.flatMap(parseNames)
    val heroRealtionCountFile=sc.textFile("../scalaTuto/data/Marvel-graph.txt")
    val heroRealtionCount=heroRealtionCountFile.map(countCoConcurrence)
    val reduceheroCount=heroRealtionCount.reduceByKey((x,y)=>x+y)
    val sorted=reduceheroCount.map(x=>(x._2,x._1)).sortByKey()
    val MostFamous=sorted.top(10)
    val lessFamous=sorted.take(10)

    var i:Int =0
    var j:Int =0
    for (res<-MostFamous){
    i+=1
      println(s"famous top ${i} : ${names.lookup(res._2)(0)} ${res._1}")

    }
    for (res <- lessFamous) {
    j+=1
      println(s"famous tail ${j} : ${names.lookup(res._2)(0)} ${res._1}")

    }
  }
}
