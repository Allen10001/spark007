package com.le.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Spark/Scala Error: value toDF is not a member of org.apache.spark.rdd.RDD
  *
  * https://community.cloudera.com/t5/Support-Questions/Spark-Scala-Error-value-toDF-is-not-a-member-of-org-apache/td-p/29878
  *
  * Ok, I finally fixed the issue. 2 things needed to be done:
  *
  * 1- Import implicits:
  *
  * Note that this should be done only after an instance of org.apache.spark.sql.SQLContext is created. It should be written as:
  *
  * val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  * import sqlContext.implicits._
  *
  * 2- Move case class outside of the method:
  *
  * case class, by use of which you define the schema of the DataFrame, should be defined outside of the method needing it. You can read more about it here:
  *
  * https://issues.scala-lang.org/browse/SI-6649
  */
object RDD2DataSetReflectionScala {

  // case class 移到方法外面，否则报错 Spark/Scala Error: value toDF is not a member of org.apache.spark.rdd.RDD
  case class Album(id: Long, name: String, screenYear: Int)

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession
      .builder
      .appName("RDD2DataSetReflectionScala")
      .master("local[3]")
      .getOrCreate

    // 使隐式转换 toDF() 生效
    import sparkSession.implicits._

    val sc = sparkSession.sparkContext

    val df = sc.textFile("hdfs://localhost:9000/user/allen/spark/input/works_album_info.txt", 3)
      .filter(line => line.split("\t").length == 23)
      .map(line => line.split("\t"))
      .map(arr => Album(arr(0).trim.toLong, arr(1).trim, arr(11).trim.toInt))
      .toDF()

    df.createOrReplaceTempView("album")

    val oldAlbumDf = sparkSession.sql("select id,name,screenYear from album where screenYear < 2000")

    val oldAlbumRDD = oldAlbumDf.rdd

    oldAlbumRDD.map(row => Album(row.getLong(0), row.getString(1), row.getInt(2)))
        .take(10)
        .foreach(album => println(album.id + ":" + album.name +":" + album.screenYear))
  }
}
