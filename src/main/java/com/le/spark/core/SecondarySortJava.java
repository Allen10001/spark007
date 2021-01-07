package com.le.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.math.Ordered;

import java.io.Serializable;

/**
 * @Description works_album_info.txt 中对记录进行二次排序，根据 screen_year 升序，screen_year相同根据 id 降序
 * 二次排序，具体实现步骤
 * 第一步：按照Ordered和Serrializable接口实现自定义排序的 Key
 * 第二步：将要进行二次排序的文件加载进来生成《key，value》类型的 RDD
 * 第三步：使用sortByKey基于自定义的Key进行二次排序
 * 第四步：去除掉排序的key，，只保留排序结果
 * @Author Allen
 * @Date 2020-04-29 10:19
 **/
public class SecondarySortJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("WorldCountLocal")
                .setMaster("local[3]");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("hdfs://localhost:9000/user/allen/spark/input/works_album_info.txt");

        lines.persist(StorageLevel.MEMORY_ONLY());

        // 过滤，只取split后长度为 23 的记录
        JavaRDD<String>  linesFiltered = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1.split("\\t").length == 23;
            }
        });

        JavaPairRDD<SecondarySort, String> javaPairRDD = linesFiltered.mapToPair(new PairFunction<String, SecondarySort, String>() {

            @Override
            public Tuple2<SecondarySort, String> call(String line){
                int screenYear = Integer.parseInt(line.split("\\t")[11]);
                int id =  Integer.parseInt(line.split("\\t")[0]);
                return new Tuple2<SecondarySort, String>(new SecondarySort(screenYear, id), line);
            }
        });

        JavaPairRDD<SecondarySort, String> sortedjavaPairRDD = javaPairRDD.sortByKey();

        JavaRDD<String> sortedlineRDD = sortedjavaPairRDD.map(new Function<Tuple2<SecondarySort, String>, String>() {

            @Override
            public String call(Tuple2<SecondarySort, String> tuple2){
                return tuple2._2;
            }
        });

        sortedlineRDD.persist(StorageLevel.MEMORY_ONLY());

        System.out.println(sortedlineRDD.take(10));
        System.out.println("sortedlineRDD count: "+sortedlineRDD.count());
        System.out.println("lines count: "+lines.count());

        sortedlineRDD.unpersist();
        lines.unpersist();
        jsc.close();
    }
}


class SecondarySort implements Ordered<SecondarySort>, Serializable {

    private int screenYear;
    private int id;

    public SecondarySort(int screenYear, int id) {
        this.screenYear = screenYear;
        this.id = id;
    }

    public int getScreenYear() {
        return screenYear;
    }

    public void setScreenYear(int screenYear) {
        this.screenYear = screenYear;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public int compare(SecondarySort that){

        if(this.screenYear != that.getScreenYear()){ //首先按照上映年份升序
            return this.screenYear - that.getScreenYear();
        }else if(this.id != that.getId()){ // 然后按照album id 降序
            return that.getId() - this.id;
        }else{ // 两个字段都相等，那么两条记录就是相等的
            return 0;
        }
    }

    @Override
    public boolean $greater(SecondarySort that){
        if(compare(that) > 0){
            return true;
        }else{
            return false;
        }
    }

    @Override
    public boolean $less(SecondarySort that){
        if(compare(that) < 0){
            return true;
        }else{
            return false;
        }
    }

    @Override
    public boolean $greater$eq(SecondarySort that){
        if(compare(that) >= 0){
            return true;
        }else{
            return false;
        }
    }

    @Override
    public boolean $less$eq(SecondarySort that){
        if(compare(that) <= 0){
            return true;
        }else{
            return false;
        }
    }

    @Override
    public int compareTo(SecondarySort that){
        return compare(that);
    }
}


