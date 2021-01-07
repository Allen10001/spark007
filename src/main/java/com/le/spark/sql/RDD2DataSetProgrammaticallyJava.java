package com.le.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description P104 通过动态构造元数据的方式，将 RDD 转为 DataSet
 * @Author Allen
 * @Date 2020-5-12 19:46
 **/
public class RDD2DataSetProgrammaticallyJava {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("RDD2DataSetProgrammaticallyJava")
                .master("local[3]").getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<String> lines = jsc.textFile("hdfs://localhost:9000/user/allen/spark/input/works_album_info.txt", 3)
                .filter(
                line -> {
                    return line.split("\\t").length == 23;
                }
        );

        // 1. 将普通的 RDD 转换为 类型为 Row的 RDD
        JavaRDD<Row> albumRDD = lines.map(
                line -> {
                    String[] arr = line.split("\\t");
                    return RowFactory.create(Long.valueOf(arr[0]), arr[1], Integer.valueOf(arr[11]));
                }
        );

        // 2. 动态构造元数据
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("id", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("screenYear", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        // 3. 构造 dataframe
        Dataset<Row> albumDs = sparkSession.createDataFrame(albumRDD, structType);

        // 注册 表名
        albumDs.createOrReplaceTempView("album");

        // 执行 sql
        Dataset<Row> oldAlbums = sparkSession.sql("select id, name, screenYear from album where screenYear < 2000");

        oldAlbums.foreach(row -> {
            System.out.println(row.getLong(0) + ":" + row.getString(1) + ":" + row.getInt(2));
        });
    }
}
