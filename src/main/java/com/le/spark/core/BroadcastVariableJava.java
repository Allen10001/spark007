package com.le.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * @Description 广播变量 java
 * @Author Allen
 * @Date 2020-04-28 17:39
 **/
public class BroadcastVariableJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Broadcast Demo")
                .setMaster("local[4]");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        int factor = 2;
        Broadcast<Integer> broadcastFactor = jsc.broadcast(factor);

        List<Integer> srcData = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> javaRDD = jsc.parallelize(srcData, 3);

        JavaRDD<Integer> multiRdd = javaRDD.map(new Function<Integer, Integer>() {

            @Override
            public Integer call(Integer v1) throws Exception {
                return v1*broadcastFactor.getValue();
            }
        });

        multiRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(Thread.currentThread()+", value: "+integer);
            }
        });
        jsc.close();
    }
}
