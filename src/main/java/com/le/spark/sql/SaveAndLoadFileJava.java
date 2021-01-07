package com.le.spark.sql;

import org.apache.spark.sql.*;

/**
 * @Description P106 load 和 save 操作
 * @Author Allen
 * @Date 2020-5-13 09:57
 **/
public class SaveAndLoadFileJava {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("RDD2DataSetReflection")
                .master("local[3]").getOrCreate();

        SQLContext sqlContext = sparkSession.sqlContext();

        Dataset<Row> dsFromSession = sparkSession.read().format("json").load("/Users/allen/bigdataapp/spark007/src/resources/input/students.json");
        Dataset<Row> dsFromContext = sqlContext.read().format("json").load("/Users/allen/bigdataapp/spark007/src/resources/input/students.json");

        // sparkSession和sqlContext 最终都是使用 DataFrameWriter
        dsFromSession.select("name", "age")
                .write()
                .mode(SaveMode.ErrorIfExists)
                .format("parquet")
                .save("/Users/allen/bigdataapp/spark007/src/resources/output1/");

        dsFromContext.select("name", "age")
                .write()
                .mode(SaveMode.ErrorIfExists)
                .format("parquet")
                .save("/Users/allen/bigdataapp/spark007/src/resources/output2/");

    }
}
