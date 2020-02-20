package com.hbl.flink.Streaming.transformation;

import com.hbl.flink.Streaming.custormSource.MyNoParalleSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @PackageName:IntelliJ IDEA
 * @ClassName:StreamingDemoWithMyNoParalleSource
 * @Description:
 * Union演示
 * @Author:hanbolin
 * @Data:2020/01/21/10:50
 */
public class StreamingDemoWithUnion {
    public static void main(String[] args) throws Exception {
        // 获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取数据源，自定义实现的数据源 // 注意：针对此source，并行度只能设置1
        DataStreamSource<Long> text1 = env.addSource(new MyNoParalleSource()).setParallelism(1);

        DataStreamSource<Long> text2 = env.addSource(new MyNoParalleSource()).setParallelism(1);

        // 使用union连接数据
        DataStream<Long> union = text1.union(text2);

        //进行简单的数据处理
        DataStream<Long> num = union.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("原始接收到数据：" + value);
                // 返回value
                return value;
            }
        });

        // 执行filter过滤，满足条件的数据会被留下
        DataStream<Long> filterData = num.filter(new FilterFunction<Long>() {
            // 把所有的奇数过滤掉
            @Override
            public boolean filter(Long value) throws Exception {
                // 对数据 % 2 等于0 的是偶数，返回true，否则返回false过滤
                return value % 2 == 0;
            }
        });

        DataStream<Long> resultData = filterData.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("过滤之后的数据：" + value);
                // 返回value
                return value;
            }
        });

        // 每2秒中处理一次数据
        SingleOutputStreamOperator<Long> sum = resultData.timeWindowAll(Time.seconds(2)).sum(0);

        // 打印全部的结果,设置并行度
        sum.print().setParallelism(1);

        // 获取类名
        String simpleName = StreamingDemoWithUnion.class.getSimpleName();
        env.execute(simpleName);
    }
}
