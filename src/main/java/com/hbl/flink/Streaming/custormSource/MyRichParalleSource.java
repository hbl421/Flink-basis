package com.hbl.flink.Streaming.custormSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @PackageName:IntelliJ IDEA
 * @ClassName:MyRichParalleSource
 * @Description:
 * 自定义多并行度的source第二种方法继承RichParallelSourceFunction 会额外多两个方法open和close
 * 针对source中如果需要获取其他链接资源那么可以在open方法中获取资源链接
 * 在close中关闭资源链接
 * @Author:hanbolin
 * @Data:2020/01/21/11:34
 */
public class MyRichParalleSource extends RichParallelSourceFunction<Long> {

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

    /**
     * 这个方法只会在最开始的时候被调用一次
     * 实现获取连接的代码
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open---------------");
        super.open(parameters);
    }

    /**
     * 实现关闭连接的代码
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
    }
}
