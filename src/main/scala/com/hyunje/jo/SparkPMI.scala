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
    val inputPath = "/twitter/*/*.log"

    def main(args: Array[String]): Unit = {

        //Load Data
        val configuration = new SparkConf().setAppName("Spark PMI").setMaster("yarn-cluster")
        val sparkContext = new SparkContext(configuration)

        //Counts words first.
        val wordcounts = sparkContext.textFile(inputPath).flatMap(line => {
            val dataElems = new ObjectMapper().readTree(line.split("\t").apply(2)).path("noun")
            val elemIter = dataElems.getElements
            val nouns = new ArrayBuffer[String]()
            while (elemIter.hasNext)
                nouns += elemIter.next().getTextValue
            nouns
        }).map(noun => (noun, 1)).reduceByKey((word1, word2) => word1 + word2)
        //Broadcast Counts of words for whole date
        val wholeWordcount = sparkContext.broadcast(wordcounts.collectAsMap())
        println("WholeWordCount : "+wholeWordcount.value)

        //Split date - nouns
        val jsonContents = sparkContext.textFile(inputPath).map(tweet => {
            val timestamp = tweet.substring(0, 10)
            val nouns = tweet.split("\t").apply(2)
            (timestamp, nouns)
        })

        //Count tweets by Date
        val numOfTweetsForEachDate = sparkContext.broadcast(jsonContents.groupByKey().map(date_json => {
            (date_json._1, date_json._2.size)
        }).collectAsMap())
        println("numOfTweetsForEachDate : " + numOfTweetsForEachDate.value)

        //Sum all count of tweets
        val totalTweetCount = sparkContext.broadcast(numOfTweetsForEachDate.value.foldLeft(0)(_ + _._2))

        println("TotalTweetCount : " + totalTweetCount.value)


        //Extract from JSON data
        val jsonData = jsonContents.map(dateAndNouns => {
            val dataElems = new ObjectMapper().readTree(dateAndNouns._2).path("noun")
            val elemIter = dataElems.getElements
            val elemString = new ArrayBuffer[String]()
            //            println("Size : " + dataElems.size())
            while (elemIter.hasNext)
                elemString += elemIter.next().getTextValue
            (dateAndNouns._1, elemString.mkString(" ").toString)
        })

        //Group by Key(<Date,Word>)
        val groupedByDate = jsonData.reduceByKey((pre, post) => pre + " " + post)

        //Count by word
        val wordsByDate = groupedByDate.flatMap((dateAndNouns) => {
            val nounArray = new ArrayBuffer[(String, String)]()
            dateAndNouns._2.split(" ").foreach(noun => {
                nounArray.prepend((dateAndNouns._1, noun))
            })
            nounArray
        })

        val wordcountByDate = wordsByDate.map(date_word_tuple => ((date_word_tuple._1, date_word_tuple._2), 1)).reduceByKey((a, b) => a + b)
        //        wordcountByDate.saveAsTextFile("/twitter-mid")

        //Broadcast
        wordcountByDate.persist()

        //Calculate PMI for each date and word
        //forEachDate : (Date, Iterable[((Date, Word), Count of Words in Date)]
        val date_word_PMI = wordcountByDate.groupBy(data => data._1._1).flatMap(forEachDate => {
            val countOfWordsForCurrentDate = numOfTweetsForEachDate.value.apply(forEachDate._1) //Count of tweets for current date
            //((Date, Word), Count of Words in Date)
            forEachDate._2.map(forEachWord => {
                val px: Double = wholeWordcount.value.apply(forEachWord._1._2).toDouble / totalTweetCount.value.toDouble
                val py: Double = countOfWordsForCurrentDate.toDouble / totalTweetCount.value.toDouble
                val pxy: Double = forEachWord._2.toDouble / totalTweetCount.value.toDouble
                val ixy: Double = pxy / (px * py)
                (forEachWord._1._1, forEachWord._1._2, ixy)
            })
        })

        date_word_PMI.saveAsTextFile("/twitter-mid")
    }
}