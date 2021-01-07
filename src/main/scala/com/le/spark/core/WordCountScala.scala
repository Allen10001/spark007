package com.le.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object c{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("wordCount")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("/Users/allen/bigdataapp/spark007/src/resources/spark.txt",1)

    val wordCounts = lines.flatMap(line => line.split(" "))
      .map(word=> (word, 1))
      .reduceByKey(_+_)

    wordCounts.foreach(wordCount => println("{"+wordCount._1+": "+wordCount._2+"}"))

  }
}
