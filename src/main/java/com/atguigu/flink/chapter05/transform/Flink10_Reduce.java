package com.atguigu.flink.chapter05.transform;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**Reduce算子更加通用（可以对任意类型聚合）
 * 1. 当某个key的第一个元素进来的时候, 不会触发reduce方法
 * 2. 聚合后的结果类型, 和输入类型必须一致
 *
 * @author Evan
 * @ClassName Flink09_Reduce
 * @date 2022-01-13 9:16
 */
public class Flink10_Reduce {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 10),
                new WaterSensor("sensor_1", 3L, 30),
                new WaterSensor("sensor_1", 2L, 50),
                new WaterSensor("sensor_1", 5L, 50),
                new WaterSensor("sensor_2", 5L, 50),
                new WaterSensor("sensor_2", 5L, 50)
        );

        stream
            .keyBy(WaterSensor::getId)
            .reduce(new ReduceFunction<WaterSensor>() {
                @Override
                public WaterSensor reduce(WaterSensor value1 // v1 上一次聚合的结果 v2: 这次参与聚合元素
                                        , WaterSensor value2) throws Exception {

//                    value1.setVc(value1.getVc() + value2.getVc());

                    value1.setVc(Math.max(value1.getVc(), value2.getVc()));

                    return value1;
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
