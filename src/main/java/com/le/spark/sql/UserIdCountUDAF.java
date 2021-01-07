package com.le.spark.sql;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

import java.util.Arrays;

/**
 * @Description 用户自定义聚合函数，实现统计相同userId的个数
 * @Author Allen
 * @Date 2020-5-14 15:20
 **/
public class UserIdCountUDAF extends UserDefinedAggregateFunction {

    private static final long serialVersionUID = 1L;

    /**
     * 初始化一个内部的自己定义的值,在Aggregate之前每组数据的初始化结果
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0,0);
    }

    public UserIdCountUDAF() {
        super();
    }

    /**
     * 指定输入字段的字段及类型
     */
    @Override
    public StructType inputSchema() {
        return DataTypes.createStructType(
                Arrays.asList(DataTypes.createStructField("userId", DataTypes.LongType, true)));
    }

    /**
     * 在中间进行聚合时所要处理的数据的类型
     */
    @Override
    public StructType bufferSchema() {
        return DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("idCounts", DataTypes.IntegerType, true)));
    }

    /**
     * 指定UDAF函数计算后返回的结果类型
     */
    @Override
    public DataType dataType() {
        return DataTypes.IntegerType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    /**
     * 更新 可以认为一个一个地将组内的字段值传递进来 实现拼接的逻辑
     * buffer.getInt(0)获取的是上一次聚合后的值
     * 相当于map端的combiner，combiner就是对每一个map task的处理结果进行一次小聚合
     * 大聚和发生在reduce端.
     * 这里即是: 在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        buffer.update(0, buffer.getInt(0)+1);
    }

    /**
     * 合并 update 操作，是针对一个分组内的部分数据，在某个节点上发生的. 但是可能一个分组内的数据，会分布在多个节点上处理
     * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来
     * buffer1.getInt(0) : 大聚和的时候 上一次聚合后的值
     * buffer2.getInt(0) : 这次计算传入进来的update的结果
     * 这里即是：最后在分布式节点完成后需要进行全局级别的Merge操作
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        buffer1.update(0, buffer1.getInt(0)+buffer2.getInt(0));
    }

    /**
     * 最后返回一个和输出 DataType  dataType()的类型要一致的类型，返回UDAF最后的计算结果
     */
    @Override
    public Object evaluate(Row buffer) {
        return buffer.getInt(0);
    }

    @Override
    public Column apply(Seq<Column> exprs) {
        return super.apply(exprs);
    }

    @Override
    public Column apply(Column... exprs) {
        return super.apply(exprs);
    }

    @Override
    public Column distinct(Seq<Column> exprs) {
        return super.distinct(exprs);
    }

    @Override
    public Column distinct(Column... exprs) {
        return super.distinct(exprs);
    }
}
