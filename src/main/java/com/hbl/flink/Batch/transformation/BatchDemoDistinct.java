package com.hbl.flink.Batch.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * Flink之DataSet中transformation
 * distinct去重操作
 */
public class BatchDemoDistinct {
    public static void main(String[] args) throws Exception {
        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 创建list集合
        ArrayList<String> data = new ArrayList<>();
        data.add("hello you");
        data.add("hello me");

        // 官方提供的集合数据源
        DataSource<String> text = env.fromCollection(data);

        // 使用dataset，flatmap
        DataSet<String> flatMapData = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.toLowerCase().split("\\W+");
                for (String word : split) {
                    System.out.println("单词:" + word);
                    out.collect(word);
                }
            }
        });

        // 打印全部结果,对数据进行整体去重
        flatMapData.distinct().print();

        // 执行任务
        env.execute("");
    }
}