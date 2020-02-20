package com.hbl.flink.Streaming.custormSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @PackageName:IntelliJ IDEA
 * @ClassName:StreamingDemoWithMyRichParalleSource
 * @Description:
 *实现自定义多并行度的source数据处理的第二种方法继承RichParalleSource
 * @Author:hanbolin
 * @Data:2020/01/21/11:41
 */
public class StreamingDemoWithMyRichParalleSource {
    public static void main(String[] args) throws Exception {
        // 获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 获取数据源
        DataStreamSource<Long> text = env.addSource(new MyRichParalleSource());

        // 使用map对数据进行处理
        DataStream<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到的数据:" + value);
                return value;
            }
        });

        // 每2秒对数据进行处理
        SingleOutputStreamOperator<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);

        // 打印全部结果
        sum.print().setParallelism(1);

        // 获取本类名称
        String simpleName = StreamingDemoWithMyRichParalleSource.class.getSimpleName();
        env.execute(simpleName);
    }
}
