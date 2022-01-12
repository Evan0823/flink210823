package com.atguigu.flink.chapter05.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Evan
 * @ClassName Flink01_Map
 * @date 2022-01-12 20:02
 */
public class Flink01_Map {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<Integer> stream = env.fromElements(1, 3, 8, 4, 10);

        /*stream
            .map(new MapFunction<Integer, Integer>(){

                @Override
                public Integer map(Integer value) throws Exception {
                    return value * value;
                }
            })
            .print();*/

        stream
            .map( value -> value * value)
            .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
