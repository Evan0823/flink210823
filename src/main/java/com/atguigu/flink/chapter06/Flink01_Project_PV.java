package com.atguigu.flink.chapter06;

import com.atguigu.flink.bean.UserBehavior;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**网站总浏览量PV：page view
 *      实现思路1: WordCount
 *
 * @author Evan
 * @ClassName Flink01_Project_PV
 * @date 2022-01-14 8:57
 */
public class Flink01_Project_PV {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env
            .readTextFile("input/UserBehavior.csv")
            .map(line -> {
                String[] data = line.split(",");
                // 将数据封装成JavaBean
                return new UserBehavior(
                        Long.valueOf(data[0]),
                        Long.valueOf(data[1]),
                        Integer.valueOf(data[2]),
                        data[3],
                        Long.valueOf(data[4])
                );
            })
            .filter( value -> "pv".equals(value.getBehavior()) ) //过滤出pv行为
            .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                    return Tuple2.of("pv", 1L);
                }
            })
            .keyBy(t -> t.f0)
            .sum(1)
            .print();

        try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
    }
}
