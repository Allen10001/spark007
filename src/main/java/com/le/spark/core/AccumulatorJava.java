package com.le.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

/**
 * @Description https://spark.apache.org/docs/2.4.7/api/java/index.html#org.apache.spark.util.AccumulatorV2
 * @Author Allen
 * @Date 2020-04-28 18:39
 **/
public class AccumulatorJava {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("WorldCountLocal")
                .setMaster("local[3]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        AccumulatorV2 accumulator = new LongAccumulator();

        // 使用之前必须先注册到 sparkContext
        jsc.sc().register(accumulator);

        List<Integer> srcData = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> javaRDD = jsc.parallelize(srcData, 3);

        javaRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                ((LongAccumulator) accumulator).add(integer);
            }
        });

        System.out.println(((LongAccumulator) accumulator).value());
        jsc.close();

    }

}
