package com.lu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @author LUJUHUI
 * @date 2019/7/19 15:34
 */
public class DataSet_WordCount {
    public static void main(String[] args) {
        ExecutionEnvironment dataSetEnv = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> dataSet = dataSetEnv.readTextFile("F:\\Auro_BigData\\flink0718\\input");

        FlatMapOperator<String, String> dateSet1 = dataSet.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        MapOperator<String, Tuple2<String, Integer>> dataSet2 = dateSet1.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

//        AggregateOperator<Tuple2<String, Integer>> result = dataSet2.groupBy(0).sum(1);
        DataSet<Tuple2<String, Integer>> result = dataSet2.groupBy(0).sum(1);

        try {
            result.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
