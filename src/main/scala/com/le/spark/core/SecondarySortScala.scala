package com.le.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object SecondarySortScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("lineCount").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://localhost:9000/user/allen/spark/input/works_album_info.txt")
    val pairs = lines.filter(line => line.split("\t").length == 23).map(line => (new SecondarySortKey(line.split("\t")(11).toInt, line.split("\\t")(0).toInt), line))
    // val sortedPairs = pairs.sortBy(pair => pair._1)    // 此处能达到 sortByKey 相同的效果
    val sortedPairs = pairs.sortByKey()
    val sortedLines = sortedPairs.map(line => line._2)
    println(sortedLines.take(10).toList)
  }
}

class SecondarySortKey(val first: Int, val second: Int)
  extends  Ordered[SecondarySortKey] with Serializable {

  override def compare(that: SecondarySortKey): Int = {
    if(this.first != that.first){
      this.first - that.first
    }else{
      this.second - that.second
    }
  }
}
