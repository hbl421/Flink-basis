package com.hbl.flink.Batch.transformation;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * Flink之DataSet中transformation
 * outerjoin外连接
 * 左外连接
 * 右外连接
 * 全外连接
 */
public class BatchDemoOuterJoin {
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
        data2.add(new Tuple2<>(4, "guangzhou"));

        // 添加对应的数据源
        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);
        System.out.println("*****************************左外连接******************************");
        /**
         * 左外连接
         * 注意：second这个tuple中的元素可能为空
          */
        text1.leftOuterJoin(text2).where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        // 左外连接 text1里面不管用户ID是否相等，都显示
                        if(second == null){// second的用户ID不等于first的用户id
                            return new Tuple3<>(first.f0, first.f1, "null");
                        }else{
                            return new Tuple3<>(first.f0, first.f1, second.f1);
                        }
                    }
                }).print();
        System.out.println("*****************************右外连接******************************");

        /**
         * 右外连接
         * 注意：first这个tuple中的数据可能为空
         */
        text1.rightOuterJoin(text2).where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        // 右外连接 显示text2里面不管用户id是否相等，都显示
                        if(first==null){// 前提是first的用户id不等于second的用户id
                            return new Tuple3<>(second.f0, "null", second.f1);
                        }else{
                            return new Tuple3<>(first.f0, first.f1, second.f1);
                        }

                    }
                }).print();
        System.out.println("*****************************全外连接******************************");

        /**
         * 全外连接
         */
        text1.fullOuterJoin(text2).where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        // 全外连接，first和second不管是否相同，都显示
                        if(first==null){// first的用户ID等于空，表示first里并没有这个数据，显示second
                            return new Tuple3<>(second.f0, "null", second.f1);
                        }else if(second==null){// second的用户ID等于空，表示second里并没有这个数据，显示first
                            return new Tuple3<>(first.f0, first.f1, "null");
                        }else {
                            return new Tuple3<>(first.f0, first.f1, second.f1);
                        }
                    }
                }).print();
    }
}