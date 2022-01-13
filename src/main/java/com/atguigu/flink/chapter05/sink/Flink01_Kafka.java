package com.atguigu.flink.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**将数据转成JSON格式写出到Kafka
 *
 * @author Evan
 * @ClassName Flink01_Kafka
 * @date 2022-01-13 11:03
 */
public class Flink01_Kafka {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 10),
                new WaterSensor("sensor_1", 3L, 30),
                new WaterSensor("sensor_1", 2L, 50),
                new WaterSensor("sensor_1", 5L, 50),
                new WaterSensor("sensor_2", 5L, 50),
                new WaterSensor("sensor_2", 5L, 50)
        );

        stream
            .map(new MapFunction<WaterSensor, String>() {
                @Override
                public String map(WaterSensor value) throws Exception {
                    return JSON.toJSONString(value);
                }
            })
            .addSink(new FlinkKafkaProducer<String>(
                    "hadoop102:9092",
                    "s1",
                    new SimpleStringSchema() // 写出，序列化
            ));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
