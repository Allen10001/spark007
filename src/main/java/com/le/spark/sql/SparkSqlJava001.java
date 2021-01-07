package com.le.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.spark_project.dmg.pmml.DataType;

/**
 * @Description P101
 * @Author Allen
 * @Date 2020-5-09 18:03
 **/
public class SparkSqlJava001 {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("sparkSqlDemo")
                .master("local[3]")
                .getOrCreate();

        //Dataset stuDataset = sparkSession.read().schema("id INT, name STRING, age INT").json("/Users/allen/bigdataapp/spark007/src/resources/input/students.json");
        Dataset stuDataset = sparkSession.read().json("/Users/allen/bigdataapp/spark007/src/resources/input/students.json");

        stuDataset.printSchema();

        // 条件查询
        stuDataset.select("name")
                .where("age = 12 or age = 13")
                .show(2);
        // 计算
        stuDataset.select(stuDataset.col("name"), stuDataset.col("age").plus(1)).show();

        // 过滤
        stuDataset.filter(stuDataset.col("age").gt(13)).show();
        // 分组，聚合
        stuDataset.groupBy("name").count().show();

        sparkSession.close();
    }
}
