package com.le.spark.core

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorScala {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("ACCUMULATOR")
      .setMaster("local[3]")

    val sc = new SparkContext(conf)
    val sumAccumulator = new LongAccumulator()
    sc.register(sumAccumulator,"Num Accumulator")

    val numbers = Array(1,2,3,4,5,6,7,8,9,10);
    val numberRDD = sc.parallelize(numbers,3)

    val sumRdd = numberRDD.foreach(item => sumAccumulator.add(item))

    println("sumAccumulator.name: "+ sumAccumulator.name + ", sumAccumulator.value: "+ sumAccumulator.value)
  }
}
