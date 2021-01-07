package com.le.spark.sql;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.parser.SqlBaseParser;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import static org.apache.spark.sql.functions.*;
import java.util.Arrays;
import java.util.List;

/**
 * @Description P109 parquet 数据源之合并元数据
 *
 * P 110 join
 * @Author Allen
 * @Date 2020-5-13 11:35
 **/
public class ParquetMergeSchemaJava {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("RDD2DataSetReflection")
                .master("local[3]")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        // name， age
        Dataset<Row> stuDs1 = sparkSession
                .read()
                .parquet("/Users/allen/bigdataapp/spark007/src/resources/output1/part-00000-72f31f97-d09c-4720-9b1d-8e5103d69a2e-c000.snappy.parquet");
        /**
         * root
         *  |-- name: string (nullable = true)
         *  |-- age: long (nullable = true)
         */
        stuDs1.printSchema();
        stuDs1.write()
                .mode(SaveMode.Overwrite)
                .format("parquet")
                .save("hdfs://localhost:9000/user/allen/spark/output3/");

        List<Tuple2<String, Integer>> students = Arrays.asList(new Tuple2<String, Integer>("marry", 98),
                new Tuple2<String, Integer>("Billy", 89),
                new Tuple2<String, Integer>("lisi", 13)
        );
        JavaRDD<Tuple2<String, Integer>> stuRDD = jsc.parallelize(students, 3);

        JavaRDD<Row> rowRDD = stuRDD.map(tuple->{
            return RowFactory.create(tuple._1, tuple._2);
        });

        // 构造元数据
        List<StructField> structFields = Lists.newArrayList(
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("score", DataTypes.IntegerType, true)
        );
        StructType structType = DataTypes.createStructType(structFields);
        // 创建新的有不同 schema 的 df
        Dataset<Row> stuDs2 = sparkSession.createDataFrame(rowRDD, structType);
        /**
         * root
         *  |-- name: string (nullable = true)
         *  |-- score: integer (nullable = true)
         */
        stuDs2.printSchema();
        stuDs2.write()
                .format("parquet")
                .mode(SaveMode.Append)
                .save("hdfs://localhost:9000/user/allen/spark/output3/");


        Dataset<Row> mergedStudents = sparkSession.read()
                .option("mergeSchema", "true")
                .parquet("hdfs://localhost:9000/user/allen/spark/output3/");
        /**
         * root
         *  |-- name: string (nullable = true)
         *  |-- age: long (nullable = true)
         *  |-- score: integer (nullable = true)
         */
        mergedStudents.printSchema();



        /*Dataset<Row> mergedStudents = sparkSession.read()
                //.option("mergeSchema", "true")
                .parquet("hdfs://localhost:9000/user/allen/spark/output3/");
        *//**
         * root
         *  |-- name: string (nullable = true)
         *  |-- score: integer (nullable = true)
         *//*
        mergedStudents.printSchema();*/

        mergedStudents.show();

        /**
         * 根据 name 列内连接
         */
        Dataset<Row> stuDsInnerJoin = stuDs1.join(stuDs2, "name");
        stuDsInnerJoin.show();

        /**
         * 两个列名相同需要加 Dataset 对象名前缀，
         * 不同的话可以直接用 col
         */
        //Dataset<Row> stuDsOutJoin = stuDs1.join(stuDs2, stuDs1.col("name").equalTo(stuDs2.col("name")), "left_outer");
        Dataset<Row> stuDsOutJoin = stuDs1.join(stuDs2, col("age").equalTo(col("score")), "left_outer");
        stuDsOutJoin.show();

    }
}
