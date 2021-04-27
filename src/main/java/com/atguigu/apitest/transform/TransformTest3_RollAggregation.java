package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author spin
 * @date 2021/4/26 19:11
 * @description: TODO
 */
public class TransformTest3_RollAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据
        DataStream<String> inputStream = env.readTextFile("F:\\computer\\java\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fileds = line.split(",");
            return new SensorReading(fileds[0], new Long(fileds[1]), new Double(fileds[2]));
        });

        //分组
        KeyedStream<SensorReading, Tuple> keydStream = dataStream.keyBy("id"); //按照id做了分组

        //reduce聚合 取最大的温度值以及最新的时间戳
        SingleOutputStreamOperator<SensorReading> resultStream = keydStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(
                        value1.getId(),
                        value2.getTimestamp(),
                        Math.min(value1.getTemperature(), value2.getTemperature())
                );
            }
        });
        resultStream.print();
        env.execute();
    }
}