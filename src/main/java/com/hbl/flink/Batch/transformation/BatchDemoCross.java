package com.hbl.flink.Batch.transformation;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * Flink之DataSet中transformation
 * Cross演示
 * 获取笛卡尔乘积
 */
public class BatchDemoCross {
    public static void main(String[] args) throws Exception {
        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // tuple2<用户id， 用户姓名>
        ArrayList<String> data1 = new ArrayList<>();
        data1.add("zs");
        data1.add("ww");

        // tuple2<用户id， 用户所在城市>
        ArrayList<Integer> data2 = new ArrayList<>();
        data2.add(1);
        data2.add(2);

        // 添加对应的数据源
        DataSource<String> text1 = env.fromCollection(data1);
        DataSource<Integer> text2 = env.fromCollection(data2);

        // cross笛卡尔积
        CrossOperator.DefaultCross<String, Integer> cross = text1.cross(text2);

        // 打印数据
        cross.print();
    }
}