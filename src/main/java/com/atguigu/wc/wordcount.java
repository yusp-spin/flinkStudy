package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author spin
 * @date
 */
//批处理
public class wordcount {

    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String inputPath = "F:\\computer\\java\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        //对数据集进行处理,按空格分词展开，转换成(word,1)这样的二元组进行统计
        DataSet<Tuple2<String,Integer>> resultSet= inputDataSet.flatMap(new MyflatMapper())
                .groupBy(0)//安装第一个位置的word分组
                .sum(1);//将第二个位置的数据求和
        resultSet.print();
    }

    public static class MyflatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //按空格分词
            String[] words = value.split(" ");
            for(String word:words) {
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
