package com.le.spark.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description P136 window 滑动窗口函数
 * P137 DStream 的所有计算都是由output触发的 print、foreachRDD、saveAsTextFile，saveAsObjectFile, saveAsHadoopFile
 * 由output 触发计算逻辑，由RDD的 action 操作触发每个batch的计算逻辑，缺一不可。
 * @Author Allen
 * @Date 2020-5-19 21:09
 **/
public class WindowFunctionJava {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[3]")
                .appName("UpdateStateByKey")
                .getOrCreate();

        JavaStreamingContext jsc = new JavaStreamingContext(new JavaSparkContext(sparkSession.sparkContext()),
                Durations.seconds(5));

        Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("group.id", "spark_streaming_direct_group_id_003");

        kafkaParams.put("enable.auto.commit", true);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("max.poll.records", 10);
        kafkaParams.put("heartbeatInterval", 1000);
        kafkaParams.put("session.timeout.ms", 10000);
        kafkaParams.put("auto.offset.reset","latest");

        List<String> topics = Arrays.asList("demo001");
        JavaInputDStream<ConsumerRecord<String, String>> javaInputDStream = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));

        // 获取每行
        JavaDStream<String> lines = javaInputDStream.map(consumerRecord -> {
            return consumerRecord.value();
        });

        // 获取单词
        JavaDStream<String> words = lines.flatMap(line -> {
            return Arrays.asList(line.split(",")).iterator();
        });

        JavaPairDStream<String, Integer> wordPairs = words.mapToPair(word -> {
            return new Tuple2<String, Integer>(word, 1);
        });

        /**
         * 每隔 10s 统计之前 60s 的窗口数据
         */
        JavaPairDStream<String, Integer> wordCounts = wordPairs.reduceByKeyAndWindow((Integer v1, Integer v2) ->{
            return v1+v2;
        }, Durations.seconds(60), Durations.seconds(10));

        /** Persist RDDs of this DStream with the default storage level (MEMORY_ONLY_SER) */
        // wordCounts.persist();

        /**
         * Enable periodic checkpointing of RDDs of this DStream.
         */
        // wordCounts.checkpoint(Durations.seconds(60));

        wordCounts.foreachRDD(
                wordCountsRdd ->{
                    wordCountsRdd.foreach(item-> System.out.println("key: "+item._1+", value: "+item._2));
                }
        );

        jsc.start();
        try{
            jsc.awaitTermination();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            jsc.close();
        }
    }
}
