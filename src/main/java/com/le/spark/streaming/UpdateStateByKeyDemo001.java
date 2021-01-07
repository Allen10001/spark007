package com.le.spark.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.*;

/**
 * @Description P134: updateStateByKey  transformToPair   transform
 * @Author Allen
 * @Date 2020-5-19 11:16
 **/
public class UpdateStateByKeyDemo001 {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .master("local[3]")
                .appName("UpdateStateByKey")
                .getOrCreate();

        JavaStreamingContext jsc = new JavaStreamingContext(new JavaSparkContext(sparkSession.sparkContext()),
                Durations.seconds(5));

        /** 开启 checkpoint 机制
        * Sets the context to periodically checkpoint the DStream operations for master fault-tolerance.
        * The graph will be checkpointed every batch interval.
         * 每个 batch interval 都会保存 context 到 checkpointFile 中
         * */
        jsc.checkpoint("hdfs://localhost:9000/user/allen/spark/state_checkpoint");

        // 黑名单
        List<Tuple2<String, Boolean>> blackList = Arrays.asList(
                new Tuple2<String, Boolean>("hello", false),
                new Tuple2<String, Boolean>("hadoop", true),
                new Tuple2<String, Boolean>("spark", true)
                );

        final JavaPairRDD<String, Boolean> blackWordsRdd = jsc.sparkContext().parallelizePairs(blackList);

        Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("group.id", "spark_streaming_direct_group_id_003");

        kafkaParams.put("enable.auto.commit", true);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("max.poll.records", 10);
        kafkaParams.put("heartbeatInterval", 1000);
        kafkaParams.put("session.timeout.ms", 10000);

        /**
         * // todo sparkStreaming 不支持事务 ？？
         * http://apache-spark-user-list.1001560.n3.nabble.com/Spark-Kafka-Streaming-With-Transactional-Messages-td37710.html
         */
        // kafkaParams.put("isolation.level","read_uncommitted");
        /**
         * What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server
         * (e.g. because that data has been deleted):
         * <ul>
         * <li>earliest: automatically reset the offset to the earliest offset
         * <li>latest: automatically reset the offset to the latest offset</li>
         * <li>none: throw exception to the consumer if no previous offset is found for the consumer's group</li>
         * <li>anything else: throw exception to the consumer.
         *
         * 简单理解：
         * 1，earliest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
         * 2，latest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
         * 提交过offset，latest和earliest没有区别，但是在没有提交offset情况下，用latest直接会导致无法读取旧数据。
         */
        kafkaParams.put("auto.offset.reset","latest");

        List<String> topics = Arrays.asList("demo001");

        /**
         * [Spark 2.x kafka LocationStrategies 的几种方式](https://blog.csdn.net/chuhui1765/article/details/100670414)
         * > 1. LocationStrategies.PreferBrokers()
         * 仅仅在你 spark 的 executor 在相同的节点上，优先分配到存在 kafka broker 的机器上；
         * > 2. LocationStrategies.PreferConsistent();
         *  大多数情况下使用，一致性的方式分配分区所有 executor 上。（主要是为了分布均匀）
         *
         * > 3. LocationStrategies.PreferFixed(hostMap: collection.Map[TopicPartition, String])
         * > 4. LocationStrategies.PreferFixed(hostMap: ju.Map[TopicPartition, String])
         *  如果你的负载不均衡，可以通过这两种方式来手动指定分配方式，其他没有在 map 中指定的，均采用 preferConsistent() 的方式分配；
         */
        JavaInputDStream<ConsumerRecord<String, String>> javaInputDStream = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));

        // todo 在使用 updateStateByKey 的情况下如何 手动提交 offset ？？  应该是不行的，可以使用第三方存储工具辅助实现 updateStateByKey 的功能
        // 得到 rdd 各个分区对应的 offset, 并保存在 offsetRanges 中
        //final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();

        /*javaInputDStream.foreachRDD(consumerRecordJavaRDD ->{
            final OffsetRange[] ranges = ((HasOffsetRanges)consumerRecordJavaRDD.rdd()).offsetRanges();
            offsetRanges.set(ranges);
            // 提交分区 offset
            ((CanCommitOffsets)javaInputDStream.inputDStream()).commitAsync(offsetRanges.get());
        });*/

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
         * 第一个参数 values： 当前这个 batches 计算的时候，这个key的新的值
         * 第二个参数 state： 这个key之前的状态
         */
        JavaPairDStream<String, Integer> wordCounts = wordPairs.updateStateByKey(

            (List<Integer> values, Optional<Integer> state)->{

                // 全局的单词计数
                Integer newValue = 0;

                // 如果state存在，这个这个key之前统计过单词计数，初始化本次单词计数的初始值
                if(state.isPresent()){
                    newValue = state.get();
                }

                // 累加本次新出现的值
                for(Integer value : values){
                    newValue += value;
                }
                return Optional.of(newValue);
            }
        );

        wordCounts.transformToPair(
                new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>(){

            @Override
            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> v1) throws Exception {
                // 这个报错可忽略，是idea的原因
                JavaPairRDD<String, Tuple2<Integer, Optional<Boolean>>> javaPairRDD = v1.leftOuterJoin(blackWordsRdd);
                return javaPairRDD.filter(item1 -> {
                    if(item1._2._2.isPresent() && item1._2._2.get() == true){
                        return false;
                    }else{
                        return true;
                    }
                }).mapToPair(item2 ->{
                    return new Tuple2(item2._1, item2._2._1);
                });
            }
        }
        ).foreachRDD(stringIntegerJavaPairRDD -> {
            stringIntegerJavaPairRDD.collect().forEach(item -> System.out.println("word: "+item._1+", count: "+item._2));
        });

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
