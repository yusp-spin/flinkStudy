package com.atguigu.apitest.source;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.shaded.akka.org.jboss.netty.channel.ExceptionEvent;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author spin
 * @date 2021/4/26 10:35
 * @description: TODO
 */
public class SourceTest4_UDF {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> dataStream = env.addSource(new MySensorSource());

        dataStream.print();
        env.execute();
    }

    //实现自定义的sourceFunction
    public static class MySensorSource implements SourceFunction<SensorReading>{
        private boolean running = true;
        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            Random random = new Random();

            Map<String,Double> sensorTempMap = new HashMap<>();

            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20); //高斯随机数
            }

            while(running) {
                for(String sensorId : sensorTempMap.keySet()) {
                    Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId,newTemp);
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

