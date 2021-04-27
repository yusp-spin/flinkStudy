package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @author spin
 * @date 2021/4/26 20:05
 * @description: TODO
 */
public class TransformTest4_Multi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据
        DataStream<String> inputStream = env.readTextFile("F:\\computer\\java\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fileds = line.split(",");
            return new SensorReading(fileds[0], new Long(fileds[1]), new Double(fileds[2]));
        });

        //split 分为高温、低温，按照30为界
//        dataStream.split(new OutputSelector<SensorReading>() {
//            @Override
//            public Iterable<String> select(SensorReading value) {
//                return (value.getTemperature()>30)? Collections.singletonList("high") :
//                        Collections.singletonList("low");
//            }
//        });
        SplitStream<SensorReading> split = dataStream.split(s -> {
            return (s.getTemperature() > 30) ? Collections.singletonList("high") :
                    Collections.singletonList("low");
        });
        DataStream<SensorReading> high = split.select("high");
        DataStream<SensorReading> low = split.select("low");
        DataStream<SensorReading> all = split.select("high", "low");


        //2. 合流 connect ，将高温流转换成二元组类型，与低温流连接合并后，输出状态信息
        DataStream<Tuple2<String, Double>> warningStream = high.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = warningStream.connect(low);
        DataStream<Object> dataStream1 = connect.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "warning");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), "healthy");
            }
        });
//        dataStream1.print();
        DataStream<SensorReading> union = high.union(low);
        dataStream.print("all");
        union.print("union");
        env.execute();

    }
}
