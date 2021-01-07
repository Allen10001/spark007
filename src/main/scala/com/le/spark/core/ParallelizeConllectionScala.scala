package com.le.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object ParallelizeConllectionScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("parallelizeCollection")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)

    val numbers = Array(1,2,3,4,5,6,7,8,9,10);
    val numberRDD = sc.parallelize(numbers,4)
    val sum = numberRDD.reduce(_+_)

    println(sum)

  }
}
