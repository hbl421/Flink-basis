package com.hbl.flink.Streaming.custormPartition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @PackageName:IntelliJ IDEA
 * @ClassName:Mypartition
 * @Description:
 * 定义自定义分区
 * @Author:hanbolin
 * @Data:2020/01/22/10:04
 */
public class Mypartition implements Partitioner<Long> {
    @Override
    public int partition(Long key, int numPartition) {
        System.out.println("分区总数:" + numPartition);
        if (key % 2 == 0) {
            return 0;
        } else {
            return 1;
        }
    }
}
