package com.le.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 广播变量 scala
  */
object BroadcastVariableScala {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("parallelizeCollection")
      .setMaster("local[3]")
    val sc = new SparkContext(conf)

    val factor = 2
    val factorBroadcast = sc.broadcast(factor)

    val arr = Array(1,2,3,4,5,6,7,8,9,10)
    val arrRdd = sc.parallelize(arr,3)
    val multiRdd = arrRdd.map(item => item*factorBroadcast.value)

    multiRdd.foreach(item => print(" "+item))

  }
}
