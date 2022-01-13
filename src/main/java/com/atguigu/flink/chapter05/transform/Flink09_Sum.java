package com.atguigu.flink.chapter05.transform;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * select id, sum(vc), 'aaa' from t group id;
 *
 * sum max min
 * 0. 必须先keyKey再使用聚合算子
 * 1. 聚合的字段必须是数字类型(int long double ...)
 * 2. 对非分组字段和聚合字段, 取的是碰到的第一个值(如ts)---sql中对于非分组字段是不能跟在select后面的
 *
 * maxBy minBy
 * 当最大值相等的时候, 默认取的是第一个(如ts)
 *                  如何取最新的? 传参数false
 *
 * @author Evan
 * @ClassName Flink09_Sum
 * @date 2022-01-13 9:44
 */
public class Flink09_Sum {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 10),
                new WaterSensor("sensor_1", 3L, 30),
                new WaterSensor("sensor_1", 2L, 50),
                new WaterSensor("sensor_2", 4L, 100),
                new WaterSensor("sensor_1", 5L, 50),
                new WaterSensor("sensor_2", 6L, 600)
        );

        stream
            .keyBy(new KeySelector<WaterSensor, String>() {
                @Override
                public String getKey(WaterSensor value) throws Exception {
                    return value.getId();
                }
            })
            //            .sum("vc")
            //            .max("vc")
            //            .min("vc")
                .maxBy("vc",false)
            .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
