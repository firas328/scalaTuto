package com.firas.spark

import breeze.numerics.sqrt
import org.apache.log4j._
import org.apache.spark._

import java.nio.charset.CodingErrorAction
import scala.io.{Codec, Source}
object moviesSimilarities {
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
  type MovieRating=(Int,Double)
  type UserRatingPair=(Int,(MovieRating,MovieRating))
  def makePairs(userRating:UserRatingPair)={
    val movieRating1=userRating._2._1
    val movieRating2=userRating._2._2
    val movie1=movieRating1._1
    val movie2=movieRating2._1
    val rating1=movieRating1._2
    val rating2=movieRating2._2
    ((movie1,movie2),(rating1,rating2))
  }
  def ratingsFilter(userRatingPair: UserRatingPair):Boolean={
    val movie1=userRatingPair._2._1
    val movie2=userRatingPair._2._2
    movie1._2<movie1._1
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
    var numPairs: Int = 0
    var sum_xx: Double = 0.0
    var sum_yy: Double = 0.0
    var sum_xy: Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator: Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)

    var score: Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    return (score, numPairs)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc=new SparkContext("local[*]","moviesSimilarities")
    println("\n loading movies names")
    val nameDict=loadMoviesNames()
    val data=sc.textFile("../scalaTuto/data/ml-100k/u.data")
    val ratings=data.map(x=>x.split("\t")).map(x=>(x(0).toInt,(x(1).toInt,x(2).toDouble)))
    val joinedRating=ratings.join(ratings)
    val filtredJoinedRating=joinedRating.filter(ratingsFilter)
    val moviesPairs=filtredJoinedRating.map(makePairs)
    val moviesPairsRatings=moviesPairs.groupByKey()
    val moviePairSimilarities = moviesPairsRatings.mapValues(computeCosineSimilarity).cache()

    val scoreThreshold = 0.97
    val coOccurenceThreshold = 50.0

    val movieID: Int = 50
    val filteredResults = moviePairSimilarities.filter(x => {
      val pair = x._1
      val sim = x._2
      (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
    }
    )
    val results = filteredResults.map( x => (x._2, x._1)).sortByKey(false).take(10)

    println("\nTop 10 similar movies for " + nameDict(movieID))
    for (result <- results) {
      val sim = result._1
      val pair = result._2
      // Display the similarity result that isn't the movie we're looking at
      var similarMovieID = pair._1
      if (similarMovieID == movieID) {
        similarMovieID = pair._2
      }
      println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
    }

  }
}
