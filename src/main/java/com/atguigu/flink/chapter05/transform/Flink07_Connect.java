package com.atguigu.flink.chapter05.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * connect:
 * 1. 只能两个流进行connect
 * 2. 两个流的数据类型可以不一致, 而且实际使用的时候, 大部分是不一致
 * 3. connect之后的流, 内部其实还是分开处理
 *
 * @author Evan
 * @ClassName Flink07_Connect
 * @date 2022-01-13 9:15
 */
public class Flink07_Connect {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<Integer> s1 = env.fromElements(10, 20, 50, 30, 15);
        DataStreamSource<String> s2 = env.fromElements("a", "b", "aa", "cc", "e");

        ConnectedStreams<Integer, String> s12 = s1.connect(s2);

        s12
            .map(new CoMapFunction<Integer, String, String>() {
                @Override
                public String map1(Integer value) throws Exception {
                    return value + "<";
                }

                @Override
                public String map2(String value) throws Exception {
                    return value + ">";
                }
            })
            .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
