package com.atguigu.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author spin
 * @date 2021/4/249:44
 * @description: TODO
 */
public class SourceTest2File {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从文件读取数据
        DataStream<String> dataStream = env.readTextFile("F:\\computer\\java\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //打印输出
        dataStream.print();
        env.execute();
    }
}
