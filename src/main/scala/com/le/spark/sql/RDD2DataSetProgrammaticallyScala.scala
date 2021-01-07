package com.le.spark.sql

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * P102  1. 通过反射的方式加载 dataframe
  */
object RDD2DataSetProgrammaticallyScala {
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession
      .builder
      .appName("RDD2DataSetReflectionScala")
      .master("local[3]")
      .getOrCreate

    val albumRdd = sparkSession.sparkContext.textFile("hdfs://localhost:9000/user/allen/spark/input/works_album_info.txt",3)
      .map(line => Row(line.split("\t")(0).toLong, line.split("\t")(1), line.split("\t")(11).toInt))

    val structType = StructType(Array(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("screenYear", IntegerType, true)
    ))

    val df = sparkSession.createDataFrame(albumRdd, structType)

    df.createOrReplaceTempView("album")

    val oldAlbumDf = sparkSession.sql("select id, name, screenYear from album where screenYear < 2000")

    oldAlbumDf.take(10).toList.foreach(album => println(album))
  }
}
