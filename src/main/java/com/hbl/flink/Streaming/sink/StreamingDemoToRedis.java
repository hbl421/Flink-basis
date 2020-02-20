package com.hbl.flink.Streaming.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @PackageName:IntelliJ IDEA
 * @ClassName:StreamingDemoToRedis
 * @Description:
 * 接收socket数据，把数据保存到redis中
 * 使用redis中的list数据类型
 * list
 *  lpush list_key value
 *
 * @Author:hanbolin
 * @Data:2020/01/22/10:52
 */
public class StreamingDemoToRedis {
    public static void main(String[] args) throws Exception {
        // 获取flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("bigdata2", 9000, "\n");

        //lpush l_words word

        // 对数据进行组装,把string转换为tuple2<string, sring>
        SingleOutputStreamOperator<Tuple2<String, String>> l_wordsData = text.map(new MapFunction<String, Tuple2<String, String>>() {

            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<>("l_words", value);
            }
        });

        // 创建redis的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("bigdate2").setPort(9000).build();

        // 创建redissink
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(conf, new MyRedisMapper());

        l_wordsData.addSink(redisSink);

        env.execute("StreamingDemoToRedis");
    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>>{
        // 表示从接收的数据中获取需要操作的redis key
        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f0;
        }
        // 表示从接收的数据中获取需要操作的redis value
        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f1;
        }
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }
    }
}
