package com.le.spark.streaming;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @Description P126
 * @Author Allen
 * @Date 2020-5-14 18:30
 **/
public class SparkStreamingWorldCount {

    public static void main(String[] args) throws Exception {

        SparkSession sparkSession = SparkSession.builder()
                .master("local[3]")
                .appName("SparkStreamingWorldCount")
                .getOrCreate();

        JavaStreamingContext jsc = new JavaStreamingContext(new JavaSparkContext(sparkSession.sparkContext()),
                Durations.seconds(5));


        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("127.0.0.1", 9999);

        JavaDStream<String> words = lines.flatMap(line -> {
            return Arrays.asList(line.split(",")).iterator();
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(word ->{
            return new Tuple2<String, Integer>(word, 1);
        });

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((count1, count2)->
        {
            return count1+ count2;
        });

        wordCounts.print();

        Thread.sleep(3000);

        // 必须开启start()方法
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
