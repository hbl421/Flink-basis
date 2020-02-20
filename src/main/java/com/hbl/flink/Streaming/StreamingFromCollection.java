package com.hbl.flink.Streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @PackageName:IntelliJ IDEA
 * @ClassName:StreamingFromCollection
 * @Description:把collection集合作为数据源
 * @Author:hanbolin
 * @Data:2020/01/21/10:25
 */
public class StreamingFromCollection {
    public static void main(String[] args) throws Exception {
        // 获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Integer> data = new ArrayList<>();
        data.add(10);
        data.add(15);
        data.add(20);

        // 指定数据源，flink官方实现的connect集合数据源
        DataStreamSource<Integer> collectionData = env.fromCollection(data);
        
        // 通过mao对数据进行处理
        // 也可以使用DataStream
        SingleOutputStreamOperator<Integer> num = collectionData.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }
        });

        // 直接打印 并且设置并行度
        num.print().setParallelism(1);

        env.execute("StreamingFromCollection");
    }
}
