package com.hbl.flink.Batch.transformation;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * Flink之DataSet中transformation
 *
 */
public class BatchDemoFirstN {
    public static void main(String[] args) throws Exception {
        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(2, "zs"));
        data.add(new Tuple2<>(4, "ls"));
        data.add(new Tuple2<>(3, "ww"));
        data.add(new Tuple2<>(1, "xw"));
        data.add(new Tuple2<>(1, "aw"));
        data.add(new Tuple2<>(1, "mw"));

        // 加入集合数据源
        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);

        // 获取前三条数据，按照数据插入顺序
        text.first(3).print();
        System.out.println("*****************************************");

        // 根据数据中的第一列进行分组，获取每组的前2个数据
        text.groupBy(0).first(2).print();
        System.out.println("*****************************************");

        // 根据数据中的第一列分组，再根据第二列进行组内排序[升序]，获取每组的前2个元素
        text.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();
        System.out.println("*****************************************");

        // 不分组，排序获取集合中的前3个元素，、
        text.sortPartition(0, Order.ASCENDING).sortPartition(1, Order.DESCENDING).first(3).print();
    }
}