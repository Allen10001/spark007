package com.le.spark.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description
 *
 * https://blog.csdn.net/lxhandlbb/article/details/51819792
 * 三、数据重复读取（重复消费）的情况
 * Kafka是毫无疑问的第一选择。必须精通。做Spark ，Kafka的重要性绝对比HDFS重要。
 * 在Receiver 收到数据且保存到了HDFS等持久化引擎 但是没有来得及进行updateOffsets，
 * 此时Receiver崩溃后重新启动就会通过管理Kafka的Zookeeper中元数据再次重复读取数据，但是此时Spark Streaming 认为是成功的。
 * 但是，Kafka认为是失败的（因为没有更新offset到zookeeper中），此时就会导致数据重复消费的情况。
 * 怎么解决：处理数据的时候可以访问到元数据信息，那么可以把元数据信息 写入到内存数据库。（MemReset） 查询一下 元数据信息是否被处理过，处理过就跳过。 每次处理都查。 一个很简单的内存数据结构。
 * @Author Allen
 * @Date 2020-5-15 11:37
 **/
public class KafkaDirectStreamDemo001 {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[3]")
                .appName("SparkStreamingWorldCount")
                .getOrCreate();

        JavaStreamingContext jsc = new JavaStreamingContext(new JavaSparkContext(sparkSession.sparkContext()),
                Durations.seconds(5));


        Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("group.id", "spark_streaming_direct_group_id_002");
        kafkaParams.put("enable.auto.commit", false);
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
        kafkaParams.put("auto.offset.reset","earliest");

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


        javaInputDStream.foreachRDD(consumerRecordJavaRDD -> {
            OffsetRange[] ranges = ((HasOffsetRanges)consumerRecordJavaRDD.rdd()).offsetRanges();
            JavaRDD<Row> records = consumerRecordJavaRDD.map(consumerRecord -> {

                return RowFactory.create(consumerRecord.key(), consumerRecord.value());

            });
            List<StructField> structFields = Arrays.asList(
                    DataTypes.createStructField("key", DataTypes.StringType, true),
                    DataTypes.createStructField("value", DataTypes.StringType, true));
            StructType structType = DataTypes.createStructType(structFields);
            Dataset<Row> kvDs = sparkSession.createDataFrame(records, structType);
            kvDs.createOrReplaceTempView("kafka_kv");
            Dataset<Row> selectedDs = sparkSession.sql("select key, value from kafka_kv where 1=1");
            selectedDs.show();
            /**
             * 提交分区的方式：  http://spark.apache.org/docs/2.4.7/streaming-kafka-0-10-integration.html
             * stream.foreachRDD(rdd -> {
             *   OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
             *
             *   // some time later, after outputs have completed
             *   ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
             * });
             */
            ((CanCommitOffsets)(javaInputDStream.inputDStream())).commitAsync(ranges);
        });

        jsc.start();
        try{
            jsc.awaitTermination();
        }catch (InterruptedException e){
            System.out.println("thread"+Thread.currentThread()+ "has been interrupted!");
        }finally {
            jsc.close();
        }
    }
}
