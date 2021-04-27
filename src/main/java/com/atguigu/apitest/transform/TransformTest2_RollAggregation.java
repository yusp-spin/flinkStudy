package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author spin
 * @date 2021/4/26 16:01
 * @description: TODO
 */
public class TransformTest2_RollAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据
        DataStream<String> inputStream = env.readTextFile("F:\\computer\\java\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // 转换成SensorReading类型
//        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String s) throws Exception {
//                String[] fileds = s.split(",");
//                return new SensorReading(fileds[0],new Long(fileds[1]),new Double(fileds[2]));
//            }
//        });

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fileds = line.split(",");
            return new SensorReading(fileds[0],new Long(fileds[1]),new Double(fileds[2]));
        });

        //分组
        KeyedStream<SensorReading, Tuple> keydStream = dataStream.keyBy("id"); //按照id做了分组

        dataStream.keyBy(data -> data.getId());



        //滚动聚合，取当前最大的温度值
        SingleOutputStreamOperator<SensorReading> resultStream = keydStream.minBy("temperature");

        resultStream.print();
        env.execute();
    }
}
