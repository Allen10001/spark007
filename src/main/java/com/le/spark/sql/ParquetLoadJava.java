package com.le.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @Description P107 使用 parquet 加载数据
 *
 * P 108 读取分区文件, 自动分区推断
 *
 * https://stackoverflow.com/questions/33650421/reading-dataframe-from-partitioned-parquet-file
 * sqlContext.read.parquet can take multiple paths as input.
 * If you want just day=5 and day=6, you can simply add two paths like:
 *
 * val dataframe = sqlContext
 *       .read.parquet("file:///your/path/data=jDD/year=2015/month=10/day=5/",
 *                     "file:///your/path/data=jDD/year=2015/month=10/day=6/")
 * If you have folders under day=X, like say country=XX,
 * country will automatically be added as a column in the dataframe.
 * EDIT: As of Spark 1.6 one needs to provide a "basepath"-option in order for Spark to generate columns automatically. In Spark 1.6.x the above would have to be re-written like this to create a dataframe with the columns "data", "year", "month" and "day":
 *
 * val dataframe = sqlContext
 *      .read
 *      .option("basePath", "file:///your/path/")
 *      .parquet("file:///your/path/data=jDD/year=2015/month=10/day=5/",
 *                     "file:///your/path/data=jDD/year=2015/month=10/day=6/")
 *
 * @Author Allen
 * @Date 2020-5-13 10:22
 **/
public class ParquetLoadJava {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("RDD2DataSetReflection")
                .master("local[3]").getOrCreate();

        // 加载 com/le/spark/sql/SaveAndLoadFileJava.java 输出的 parquet 数据
       /* Dataset<Row> ds = sparkSession
                .read()
                .parquet("/Users/allen/bigdataapp/spark007/src/resources/output1/part-00000-72f31f97-d09c-4720-9b1d-8e5103d69a2e-c000.snappy.parquet");
*/

       // 读取分区文件
        Dataset<Row> ds = sparkSession
                .read()
                .option("basePath", "/Users/allen/bigdataapp/spark007/src/resources/output1/")
                .parquet("/Users/allen/bigdataapp/spark007/src/resources/output1/gender=male");

        ds.createOrReplaceTempView("students");
        Dataset<Row> youngStu = sparkSession.sql("select * from students where age<13");
        youngStu.printSchema();
        youngStu.show();
    }
}
