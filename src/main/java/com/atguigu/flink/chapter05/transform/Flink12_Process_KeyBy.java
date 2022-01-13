package com.atguigu.flink.chapter05.transform;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**带有keyBy的，计算不同sensor的累加---其实是不可靠的
 *              可靠：后面学习的键控（底层其实就是Hash）
 *
 *              自己实现的一个转态管理
 * @author Evan
 * @ClassName Flink10_Process
 * @date 2022-01-13 9:17
 */
public class Flink12_Process_KeyBy {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 3L, 2),
                new WaterSensor("sensor_1", 2L, 3),
                new WaterSensor("sensor_1", 5L, 4),
                new WaterSensor("sensor_2", 5L, 5),
                new WaterSensor("sensor_2", 5L, 6)
        );

        stream
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

//                    int sum = 0;
                    Map<String,Integer> map = new HashMap<String, Integer>();
                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<WaterSensor> out) throws Exception {
                        // 状态：根据不同id去求和
                        Integer sum = map.getOrDefault(value.getId(), 0); // 有可能找不到就给个默认值

                        sum += value.getVc();

                        value.setVc(sum);

                        out.collect(value);

                        map.put(value.getId(), sum);

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
