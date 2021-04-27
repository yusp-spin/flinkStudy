package com.atguigu.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author spin
 * @date
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //从文件中读取
//        String inputPath = "F:\\computer\\java\\FlinkTutorial\\src\\main\\resources\\hello.txt";
//        DataStreamSource<String> inputDataStream = env.readTextFile(inputPath);

        //用parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");


        //流式数据能从kafka读取
        //这里不用kafka 而是用nc，从socket文本流读取数据
        DataStream<String> inputDataStream = env.socketTextStream(host,port);

        //基于数据流进行转换计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new wordcount.MyflatMapper())
                .keyBy(0)
                .sum(1);
        resultStream.print();

        //执行任务
        env.execute();
    }
}
