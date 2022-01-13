package com.atguigu.flink.chapter05.sink;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**将数据写出到Kafka(没有先转成JSON格式)
 *
 * @author Evan
 * @ClassName Flink01_Kafka
 * @date 2022-01-13 11:03
 */
public class Flink01_Kafka_1 {
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

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop102:9092");
        stream
            .addSink(new FlinkKafkaProducer<WaterSensor>(
                    "default",
                    new KafkaSerializationSchema<WaterSensor>() {
                        @Override
                        public ProducerRecord<byte[], byte[]> serialize(WaterSensor element,
                                                                        @Nullable Long timestamp) {
                            return new ProducerRecord<>("s1", element.toString().getBytes(StandardCharsets.UTF_8));
                        }
                    },
                    props,
                    FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
            ));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
