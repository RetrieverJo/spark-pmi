package com.hyunje.jo

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.mutable.ArrayBuffer


/**
 * Calculate PMI Score based on twitter Word Data
 *
 * @author hyunje
 * @since   15. 1. 16. 
 */
object SparkPMI {
  def main(args: Array[String]): Unit = {
    //Load Data
    val configuration = new SparkConf().setAppName("Spark PMI").setMaster("yarn-cluster")
    val sparkContext = new SparkContext(configuration)

    //Split date - nouns
    val jsonContents = sparkContext.textFile("/twitdata/*/*.log")
      .map(tweet => {
      val timestamp = tweet.substring(0, 10)
      val nouns = tweet.split("\t").apply(2)
      (timestamp, nouns)
    })
    //    jsonContents.saveAsTextFile("/twitter-mid")

    //Extract from JSON data
    val jsonData = jsonContents.map(dateAndNouns => {
      val dataElems = new ObjectMapper().readTree(dateAndNouns._2).path("noun")
      val elemIter = dataElems.getElements
      val elemString = new ArrayBuffer[String]()
      println("Size : " + dataElems.size())
      while (elemIter.hasNext)
        elemString += elemIter.next().getTextValue
      (dateAndNouns._1, elemString.mkString(" ").toString)
    })
    //    jsonData.saveAsTextFile("/twitter-mid")

    //Group by Key(<Date,Word>)
    val groupedByDate = jsonData.reduceByKey((pre, post) => pre + " " + post)

    //    groupedByDate.saveAsTextFile("/twitter-mid")
    //Count by word
    val wordsByDate = groupedByDate.flatMap((dateAndNouns) => {
      val nounArray = new ArrayBuffer[(String, String)]()
      dateAndNouns._2.split(" ").foreach(noun => {
        nounArray.prepend((dateAndNouns._1, noun))
      })
      nounArray
    })

    //    wordsByDate.saveAsTextFile("/twitter-mid")
    val wordcountByDate = wordsByDate.map(date_word_tuple => ((date_word_tuple._1, date_word_tuple._2), 1)).reduceByKey((a, b) => a + b)
    wordcountByDate.saveAsTextFile("/twitter-mid")
  }
}