package com.hbl.flink.Streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @PackageName:IntelliJ IDEA
 * @ClassName:StreamingFromCollection
 * @Description:
 * KafkaSource
 * @Author:hanbolin
 * @Data:2020/01/21/10:25
 */
public class StreamingKafkaSource {
    public static void main(String[] args) throws Exception {
        // 获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpoint配置
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置statebackend
        //env.setStateBackend(new Rock);

        // kafka主题是t1
        String topic = "t1";
        Properties prop = new Properties();
        // 使用bigdata2来生产数据
        prop.setProperty("bootstrap.servers", "bigdata2:9092");
        prop.setProperty("group.id", "con1");

        // 定义flink kafka消费策略
        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);
        DataStreamSource<String> text = env.addSource(myConsumer);

        myConsumer.setStartFromGroupOffsets();// 默认消费策略

        text.print().setParallelism(1);

        env.execute("StreamingFromCollection");
    }
}
