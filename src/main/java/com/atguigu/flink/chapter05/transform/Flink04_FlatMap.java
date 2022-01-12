package com.atguigu.flink.chapter05.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**flatMap算子可以替换掉map和filter算子
 *
 * @author Evan
 * @ClassName Flink04_FlatMap
 * @date 2022-01-12 20:03
 */
public class Flink04_FlatMap {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<Integer> stream = env.fromElements(1, 3, 8, 4, 10);

        stream
            .flatMap(new FlatMapFunction<Integer, Integer>() {
                @Override
                public void flatMap(Integer value,
                                    Collector<Integer> out) throws Exception {
                    // 调用两次算子，就进1个出2个
                    /*out.collect(value * value);
                    out.collect(value * value * value);*/
                    if(value % 2 == 0){
                        out.collect(value);
                    }
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
