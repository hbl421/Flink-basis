package com.hbl.flink.Streaming.transformation;

import com.hbl.flink.Streaming.custormSource.MyNoParalleSource;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @PackageName:IntelliJ IDEA
 * @ClassName:StreamingDemoWithMyNoParalleSource
 * @Description:
 * split
 *典型的应用场景
 * 根据规则把一个数据流切分为多个流
 * 可能在实际工作中，源数据流中混合了多种类似的数据
 * 多种类型的数据处理规则不一样，所以就可以在根据一定的规则
 * 把一个数据流切分成多个数据流，这样每个数据流就可以使用不同的处理逻辑了
 * @Author:hanbolin
 * @Data:2020/01/21/10:50
 */
public class StreamingDemoWithSplit {
    public static void main(String[] args) throws Exception {
        // 获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取数据源，自定义实现的数据源 // 注意：针对此source，并行度只能设置1
        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource()).setParallelism(1);

        // 对流进行切分，按照数据的奇偶性进行切分
        SplitStream<Long> splitStream = text.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> output = new ArrayList<>();
                if (value % 2 == 0) {
                    // 偶数
                    output.add("even");
                } else {
                    // 奇数
                    output.add("odd");
                }
                return output;
            }
        });

        // 选择一个或者多个切分后的流
        /*DataStream<Long> evenStream = splitStream.select("even");
        DataStream<Long> oddStream = splitStream.select("odd");*/

        DataStream<Long> moreStream = splitStream.select("even", "odd");

        // 打印全部的结果,设置并行度
        moreStream.print().setParallelism(1);

        // 获取类名
        String simpleName = StreamingDemoWithSplit.class.getSimpleName();
        env.execute(simpleName);
    }
}
