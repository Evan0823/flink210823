package com.atguigu.flink.chapter05.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * union:
 * 1. 可以把2个或多个流合并在一起
 * 2. 流的数据类型必须一致
 * 3. 合并之后, 成为一个真正的流
 *
 * @author Evan
 * @ClassName Flink08_Union
 * @date 2022-01-13 9:16
 */
public class Flink08_Union {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<String> s1 = env.fromElements("10", "20", "50", "30", "15");
        DataStreamSource<String> s2 = env.fromElements("a", "b", "aa", "cc", "e");

        DataStream<String> s12 = s1.union(s2);
        s12.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value + "<>";
            }
        }).print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
