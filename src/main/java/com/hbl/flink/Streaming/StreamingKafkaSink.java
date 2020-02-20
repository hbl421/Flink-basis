package com.hbl.flink.Streaming;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import java.util.Properties;

/**
 * @PackageName:IntelliJ IDEA
 * @ClassName:StreamingFromCollection
 * @Description:
 * KafkaSink
 * @Author:hanbolin
 * @Data:2020/01/21/10:25
 */
public class StreamingKafkaSink {
    public static void main(String[] args) throws Exception {
        // 获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用socket，在hadoop102里面获取数据，端口号是9001
        DataStreamSource<String> text = env.socketTextStream("hadoop102", 9001, "\n");

        // 使用bigdata2端口号是9092来消费hadoop102生产的数据
        String brokerList = "bigdata2:9092";
        String topic = "t1";
        //FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(brokerList, topic, new SimpleStringSchema());

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "bigdata2:9092");

        // 设置事务超时时间
        prop.setProperty("transaction.timeout.ms", 60000*15+"");

        // 使用仅一次的语义的kafkaProducer
        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(topic,
                new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()), prop,
                FlinkKafkaProducer011.Semantic.AT_LEAST_ONCE);
        text.addSink(myProducer);

        env.execute("StreamingFromCollection");
    }
}
