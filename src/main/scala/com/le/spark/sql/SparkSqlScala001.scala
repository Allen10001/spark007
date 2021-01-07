package com.le.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * P101
  */
object SparkSqlScala001 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName("sparkSqlDemo")
      .master("local[3]")
      .getOrCreate

    val ds = sparkSession.read.json("/Users/allen/bigdataapp/spark007/src/resources/input/students.json")

    ds.printSchema()

    ds.show()

    ds.select("name").show

    ds.select(ds.col("name"), ds.col("age")+1).show()

    ds.filter(ds.col("age") >= 13).show()

    ds.groupBy("name").count().show()

  }
}
