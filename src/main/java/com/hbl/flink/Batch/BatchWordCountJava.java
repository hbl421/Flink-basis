package com.hbl.flink.Batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Flink滑动窗口批处理数据
 */
public class BatchWordCountJava{
    public static void main(String[] args) throws Exception {
        // 获取数据
        String inputPath = "D:\\data\\file";
        // 打印数据位置
        String outputPath = "D:\\data\\result1";

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 获取文件中的内容
        DataSource<String> text = env.readTextFile(inputPath);
        AggregateOperator<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);

        // 可以修改他的并行度
        counts.writeAsCsv(outputPath, "\n", " ").setParallelism(1);
        env.execute("batch word count");
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>>{
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] splits = value.toLowerCase().split("\\W+");
            for (String token : splits){
                if(token.length() > 0){
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}