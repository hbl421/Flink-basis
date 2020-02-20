package com.hbl.flink.Streaming.custormSource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * @PackageName:IntelliJ IDEA
 * @ClassName:MyParalleSource
 * @Description:
 * 自定义实现一个支持并行度的source
 * @Author:hanbolin
 * @Data:2020/01/21/11:17
 */
public class MyParalleSource implements ParallelSourceFunction<Long> {

    private long count = 1L;

    private boolean isRunning = true;

    /**
     * 主要的方法
     *启动一个source
     *  大部分情况下，循环生产数据
     * @param
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while(isRunning){
            ctx.collect(count);
            // 对每一条数据加1
            count++;
            // 每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    /**
     * 取消一个cancel的时候会调用此方法
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}
