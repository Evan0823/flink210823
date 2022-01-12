package com.atguigu.flink.chapter05.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**Source--->从Java的集合中读取数据
 *
 * @author Evan
 * @ClassName Flink01_Source_List
 * @date 2022-01-12 14:48
 */
public class Flink01_Source_List {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<String> list = Arrays.asList("a", "a a", "hello", "atguigu");

        DataStreamSource<String> stream = env.fromCollection(list);

        stream.print();

        env.execute();
    }
}
