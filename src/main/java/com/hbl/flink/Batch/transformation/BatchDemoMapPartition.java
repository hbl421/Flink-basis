package com.hbl.flink.Batch.transformation;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Flink之DataSet中transformation
 * mapPartition演示
 * 一整个分区内的数据获取一次连接
 */
public class BatchDemoMapPartition {
    public static void main(String[] args) throws Exception {
        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 创建list集合
        ArrayList<String> data = new ArrayList<>();
        data.add("hello you");
        data.add("hello me");

        // 官方提供的集合数据源
        DataSource<String> text = env.fromCollection(data);

        /*text.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                // 获取数据库连接--注意，此时是每过来一条数据就获取一次连接
                // 处理数据
                // 关闭连接
                return value;
            }
        });*/

        // dataSet api
        DataSet<String> mapPartitionData = text.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
                // 获取数据库连接--注意，此时是一个分区的数据获取一次连接【优点，每个分区获取一次连接】
                // values中保存了一个分区的数据
                // 处理数据
                // 关闭连接
                Iterator<String> it = values.iterator();
                while (it.hasNext()) {
                    String next = it.next();
                    String[] split = next.split("\\W+");
                    for (String word : split) {
                        out.collect(word);
                    }
                }
            }
        });

        // 打印全部结果
        mapPartitionData.print();

        // 执行任务
        env.execute("");
    }
}