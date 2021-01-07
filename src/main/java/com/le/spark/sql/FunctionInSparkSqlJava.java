package com.le.spark.sql;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

import java.util.List;


/**
 * @Description sparkSQL 内置函数, 开窗函数，自定义函数
 * @Author Allen
 * @Date 2020-5-14 11:28
 **/
public class FunctionInSparkSqlJava {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("FunctionInSparkSqlJava")
                .master("local[3]")
                /**
                 *  agg() by
                 *  default retains the grouping columns in its output. To not retain grouping columns, set
                 *  `spark.sql.retainGroupColumns` to false.
                 */
                .config("spark.sql.retainGroupColumns",false)
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());


        // 构造用户访问日志,    date, userId
        String[] userAccessLog = {
                "2020-10-01,11211",
                "2020-10-01,11211",
                "2020-10-01,11213",
                "2020-10-02,11212",
                "2020-10-02,11213",
                "2020-10-02,11215",
                "2020-10-03,11212",
                "2020-10-03,11214",
                "2020-10-03,11215",
                "2020-10-03,11214",
                "2020-10-03,11211",
                "2020-10-04,11210",
                "2020-10-04,11219",
                "2020-10-05,11210"
        };

        JavaRDD<Row> visitInfoRdd = jsc.parallelize(Lists.newArrayList(userAccessLog), 3)
                .map(line->
                        {
                    return RowFactory.create(line.split(",")[0], Long.parseLong(line.split(",")[1]));
                    }
                );

        List<StructField> structFields = Lists.newArrayList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("userId", DataTypes.LongType, true)
        );
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> visitInfoDs = sparkSession.createDataFrame(visitInfoRdd, structType);
        // visitInfoDs.show();

        visitInfoDs.createOrReplaceTempView("user_info");

        // 缓存表，对于一条sql中可能多次使用到的表，对其进行缓存
        sparkSession.sqlContext().cacheTable("user_info");

        // 缓存 dataset
        visitInfoDs.cache();

        /*// 聚集函数
        Dataset<Row> groupByDateDs = visitInfoDs
                .groupBy("date")
                .agg(col("date"), countDistinct("userId"));
        groupByDateDs.show();
                */



  /*    *//**
     * 使用 开窗函数 取出每天的访客中 id 最大的两个用户
     * 开窗函数格式：
     * 【 rou_number() over (partitin by XXX order by XXX) 】
     *//*
        Dataset<Row> bigIdUser = sparkSession.sql("select date, userId from " +
                        "(" +
        "select date, userId, row_number() over (partition by date order by userId desc) user_id_rank from user_info" +
                        " ) temp_user_info " +
                        "where user_id_rank < 3"
        );
        bigIdUser.show();*/



        /*// 使用 udf 注册一个自定义函数, udf 是针对单行输入返回一个输出
        sparkSession.udf().register("isOdd",
                (Long userId) -> {
            if(userId % 2 == 1){
                return true;
            }else{
                return false;
            }
            },
             DataTypes.BooleanType);


        Dataset<Row> isOddDs = sparkSession.sql("select date , userId, isOdd(userId) from user_info where userId < 11215");
        isOddDs.show();*/


        /*// 自定义聚合函数
        sparkSession.udf().register("userIdCount", new UserIdCountUDAF());
        sparkSession.sql("select date, userIdCount(userId) from user_info group by date").show();*/

        visitInfoDs.show();
        visitInfoDs.take(10);

    }
}
