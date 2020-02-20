package com.hbl.flink.Streaming.custormSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @PackageName:IntelliJ IDEA
 * @ClassName:StreamingDemoWithMyParalleSource
 * @Description:
 * 使用多并行度的source,可以设置多个线程
 * @Author:hanbolin
 * @Data:2020/01/21/11:24
 */
public class StreamingDemoWithMyParalleSource {
    public static void main(String[] args) throws Exception {
        // 获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取自定义多并行度的数据源，不设置的话，根据自己的cpu核数打印，
        DataStreamSource<Long> text = env.addSource(new MyParalleSource()).setParallelism(2);

        // 使用map进行数据处理
        DataStream<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到的数据:" + value);
                return value;
            }
        });

        // 每2秒处理一次数据
        SingleOutputStreamOperator<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);

        // 打印全部的结果,并设置并行度
        sum.print().setParallelism(1);

        // 获取本类名称
        String simpleName = StreamingDemoWithMyParalleSource.class.getSimpleName();
        env.execute(simpleName);
    }
}
