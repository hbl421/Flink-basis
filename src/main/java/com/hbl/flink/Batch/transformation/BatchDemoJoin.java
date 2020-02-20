package com.hbl.flink.Batch.transformation;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * Flink之DataSet中transformation
 * join连接演示
 */
public class BatchDemoJoin {
    public static void main(String[] args) throws Exception {
        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // tuple2<用户id， 用户姓名>
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1, "张三"));
        data1.add(new Tuple2<>(2, "李四"));
        data1.add(new Tuple2<>(3, "王五"));

        // tuple2<用户id， 用户所在城市>
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1, "beijing"));
        data2.add(new Tuple2<>(2, "shanghai"));
        data2.add(new Tuple2<>(3, "guangzhou"));

        // 添加对应的数据源
        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);


        // 调用with方法
        JoinOperator.EquiJoin<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> joinData =
                text1.join(text2).where(0)// 指定第一个数据集中需要进行比较的元素角标
                .equalTo(0)// 指定第二个数据集中需要进行比较的元素角标
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        // 返回一个tuple3<第一个数据集的用户id，第一个数据集的用户姓名， 第二个数据集的用户所在城市>
                        return new Tuple3<>(first.f0, first.f1, second.f1);
                    }
                });

        // 打印全部数据
        joinData.print();

        /*// 注意：这里使用map和上面使用的with最终效果是一样的
        text1.join(text2).where(0)// 指定第一个数据集中需要进行比较的元素角标关联
                .equalTo(0)// 指定第二个数据集中需要进行比较的元素角标关联
                .map(new MapFunction<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> map(Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> value) throws Exception {
                        return new Tuple3<>(value.f0.f0, value.f0.f1, value.f1.f1);
                    }
                }).print();*/
    }
}