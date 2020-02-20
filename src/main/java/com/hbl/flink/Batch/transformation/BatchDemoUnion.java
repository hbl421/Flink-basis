package com.hbl.flink.Batch.transformation;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * Flink之DataSet中transformation
 * Union演示
 * 相同数据集连接
 */
public class BatchDemoUnion {
    public static void main(String[] args) throws Exception {
        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // tuple2<用户id， 用户姓名>
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1, "zs"));
        data1.add(new Tuple2<>(2, "ls"));
        data1.add(new Tuple2<>(3, "ww"));

        // tuple2<用户id， 用户所在城市>
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1, "lili"));
        data2.add(new Tuple2<>(2, "jack"));
        data2.add(new Tuple2<>(4, "jessic"));

        // 添加对应的数据源
        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);

        // union演示
        UnionOperator<Tuple2<Integer, String>> union = text1.union(text2);

        // 打印数据
        union.print();
    }
}