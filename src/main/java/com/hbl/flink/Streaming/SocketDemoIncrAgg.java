package com.hbl.flink.Streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author: Henry
 * @Description: window增量聚合
 * @Date: Create in 2019/5/3 9:43
 **/
public class SocketDemoIncrAgg {

    public static void main(String[] args) throws Exception{

// 设置主机名、分隔符、端口号
        String hostname = "hadoop102" ;
        String delimiter = "\n" ;
        int port ;
// 使用parameterTool，通过控制台获取参数
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port") ;
        }catch (Exception e){
            // 如果没有传入参数，则赋默认值
            System.out.println("No port set. use default port 8000--java");
            port = 8000 ;
        }

        //1、获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2、连接socket获取输入的数据，官网提供的数据源，主机名，端口号，分隔符
        DataStream<String> text = env.socketTextStream(
                hostname,port,delimiter);

        DataStream<Tuple2<Integer, Integer>> intData = text.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String value) throws Exception {
                return new Tuple2<>(1, Integer.parseInt(value));
            }
        });

        intData.keyBy(0).timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                        System.out.println("执行reduce操作:" + value1 +","+ value2);
                        return new Tuple2<>(value1.f0, value1.f1+value2.f1);
                    }
                }).print();

        // 这一行代码一定要实现，否则程序不执行
        // 报错：Unhandled exception: java.lang.Exception
        // 需要对 main 添加异常捕获
        env.execute("Socket window count");
    }

    // 自定义统计单词的数据结构，包含两个变量和三个方法
    public static class WordWithCount{
        //两个变量存储输入的单词及其数量
        public String word ;
        public long count ;

        // 空参的构造函数
        public  WordWithCount(){}

        // 通过外部传参赋值的构造函数
        public WordWithCount(String word, long count){
            this.word = word ;
            this.count = count ;
        }

        @Override
// 打印显示 word，count
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}