package com.atguigu.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;

/**
 * @author spin
 * @date 2021/4/26 14:36
 * @description: TODO
 */
public class TransformTest1Base {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据
        DataStream<String> inputStream = env.readTextFile("F:\\computer\\java\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //1.map  把string转换成长度输出
        DataStream<Integer> mapStream = inputStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });
//        mapStream.print();


        //2.flatMap 按逗号切分字段
        DataStream<String> flatmapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> ct) throws Exception {
                String[] fileds = s.split(",");
                for(String field : fileds) {
                    ct.collect(field);
                }
            }
        });
//        flatmapStream.print();

        //3.filter 筛选sensor_1开头的id对应的数据
        DataStream<String> filterStream = inputStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("sensor_1");
            }
        });
        filterStream.print();
        env.execute();
    }
}
