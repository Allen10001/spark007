package com.le.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @Description
 * @Author Allen
 * @Date 2020-04-27 18:31
 **/
public class WordCountJava {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("WorldCountLocal")
                .setMaster("local[4]");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        //JavaRDD<String> lines = jsc.textFile("/Users/allen/bigdataapp/spark007/src/resources/spark.txt");
        JavaRDD<String> lines = jsc.textFile("hdfs://localhost:9000/user/allen/spark/input/works_album_info.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String line) throws Exception{
                return Arrays.asList(line.split("\\t")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> v1){
                return new Tuple2<Integer, String>(v1._2, v1._1);
            }
        }).sortByKey()
                .mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return new Tuple2<String, Integer>(integerStringTuple2._2, integerStringTuple2._1);
            }
        });

        System.out.println(pairs.collect());

        jsc.close();
    }
}
