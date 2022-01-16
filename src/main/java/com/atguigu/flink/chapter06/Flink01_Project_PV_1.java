package com.atguigu.flink.chapter06;

import com.atguigu.flink.bean.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**实现思路2: process
 *      通过上下文getCurrentKey()过滤求和，替代filter、map算子
 *
 * @author Evan
 * @ClassName Flink01_Project_PV
 * @date 2022-01-14 8:57
 */
public class Flink01_Project_PV_1 {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env
            .readTextFile("input/UserBehavior.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new UserBehavior(
                        Long.valueOf(data[0]),
                        Long.valueOf(data[1]),
                        Integer.valueOf(data[2]),
                        data[3],
                        Long.valueOf(data[4])

                );
            })
            .keyBy(UserBehavior::getBehavior)
            .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
                long sum = 0;
                @Override
                public void processElement(UserBehavior userBehavior,
                                           Context ctx,
                                           Collector<Long> out) throws Exception {
                    String key = ctx.getCurrentKey();
                    if("pv".equals(key)){
                        sum++;
                    }
                    out.collect(sum);
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
