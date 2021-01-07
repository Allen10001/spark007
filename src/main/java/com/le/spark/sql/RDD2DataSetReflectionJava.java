package com.le.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * @Description P102  1. 通过反射的方式加载 dataframe
 * @Author Allen
 * @Date 2020-5-10 15:04
 **/
public class RDD2DataSetReflectionJava {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("RDD2DataSetReflection")
                .master("local[3]").getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        //SQLContext sqlContext = new SQLContext(jsc);

        // sparkSession.read().format("csv").option("header", "false").option("mode", "FAILFAST").schema();
        JavaRDD<String> lines = jsc.textFile("hdfs://localhost:9000/user/allen/spark/input/works_album_info.txt", 3);

        JavaRDD<String> filteredlines = lines.filter(
                line -> {
                    return line.split("\\t").length == 23;
                }
        );

        JavaRDD<AlbumBean> albumRdd = filteredlines.map(
                line->{
                    // id name screenYear
                    return new AlbumBean(Long.parseLong(line.split("\\t")[0]), line.split("\\t")[1], Integer.parseInt(line.split("\\t")[11]));
                }
        );

        // 用 java 写 spark 程序时没有 DataFrame, Dataset<Row> 就是 DataFrame
        Dataset<Row> albumDf = sparkSession.createDataFrame(albumRdd, AlbumBean.class);

        // 将 dataframe 以 csv 文件的格式写出到 hdfs
        // albumDf.write().format("csv").mode("overwrite").option("sep", "\t").save("hdfs://localhost:9000/user/allen/spark/output1/");

        // 某个字段的统计信息
        albumDf.describe("id").show();

        albumDf.registerTempTable("album");

        // 分析性能
        sparkSession.sql("select id,name,screenYear from album where screenYear < 2000").explain();
        Dataset oldAlbum = sparkSession.sql("select id,name,screenYear from album where screenYear < 2000");

        // dataframe 转为 rdd
        JavaRDD<Row> oldAlbumRdd = oldAlbum.javaRDD();
        // rdd 中的 Row 映射为 AlbumBean
        JavaRDD<AlbumBean> oldAlbumBeanRdd = oldAlbumRdd.map(row->{
            return new AlbumBean(row.getLong(0), row.getString(1), row.getInt(2));
        });

        // 要求 Bean 可以被序列化
        List<AlbumBean> lists = oldAlbumBeanRdd.collect();

        lists.forEach(albumBean -> {
            System.out.println(albumBean);
        });


        // albumDf.describe().show();
        jsc.close();
    }
}
