package com.hbl.flink.Streaming.custormPartition;

import com.hbl.flink.Streaming.custormSource.MyNoParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @PackageName:IntelliJ IDEA
 * @ClassName:StreamingDemoWithMyPartition
 * @Description:
 * 使用自定义分区
 * 根据数字的奇偶性来分区
 * @Author:hanbolin
 * @Data:2020/01/22/10:06
 */
public class StreamingDemoWithMyPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource());

        // 对数据进行转换，把long类型转成tuple1类型

        SingleOutputStreamOperator<Tuple1<Long>> tupleData = text.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long value) throws Exception {
                return new Tuple1<>(value);
            }
        });

        // 分区之后的数据
        DataStream<Tuple1<Long>> partitionData = tupleData.partitionCustom(new Mypartition(), 0);

        DataStream<Long> result = partitionData.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("当前线程id:" + Thread.currentThread().getId() + ", value:" + value);
                return value.getField(0);
            }
        });

        result.print().setParallelism(1);

        String simpleName = StreamingDemoWithMyPartition.class.getSimpleName();
        env.execute(simpleName);
    }
}
