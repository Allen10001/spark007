package com.le.spark.core

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object LineCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("lineCount").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // val lines = sc.textFile("/Users/allen/bigdataapp/spark007/src/resources/spark.txt",3)

    // 读取 hdfs://localhost:9000/user/allen/spark/output1/ 文件夹下所有的数据
    val lines = sc.textFile("hdfs://localhost:9000/user/allen/spark/output1/",3)
    println("lines count:"+ lines.count())

    val linePairs = lines.map(line => (line, 1))
    val lineCounts = linePairs.reduceByKey(_+_)

    lineCounts.persist(StorageLevel.MEMORY_ONLY_SER_2)
    //lineCounts.cache()

    //lineCounts.foreach(lineCount => println("{"+lineCount._1+": " + lineCount._2 + "}"))
    //println(lineCounts.collect().toList)
    //lineCounts.saveAsTextFile("/Users/allen/bigdataapp/spark007/src/resources/result")

   val lineCountsAfterFilter = lineCounts.filter(lineCount => (lineCount._1.contains("hadoop")))
    println(lineCountsAfterFilter.take(5).toList)

    lineCounts.unpersist(false)
    println("------------------------")
    lineCountsAfterFilter.foreach(item => println("{"+item._1+": " + item._2 + "}"))
    println(lineCountsAfterFilter.countByKey())

  }
}
