package com.le.spark.sql;

import java.math.BigDecimal;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.spark_project.dmg.pmml.DataType;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

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
        Dataset<Row> stuDataset = sparkSession.read().json("/Users/hubo88/allen_mac/bigdata_app/spark007/src/resources/input/students.json");

        stuDataset.printSchema();

        Iterable<Row> rows = stuDataset.collectAsList();
        // list 字段处理
        for (Row row : rows) {
            if (row.getAs("group") instanceof WrappedArray) {
                WrappedArray arr = (WrappedArray)(row.getAs("group"));
                JavaConversions.seqAsJavaList(arr).stream().forEach(value -> {
                    Object wrapValue = value;
                    System.out.print((new BigDecimal(wrapValue.toString())).intValue());
                    System.out.print("   ");
                });
            } else {
                System.out.println(row.getAs("group").getClass());
                System.out.println("else");
            }

        }

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
