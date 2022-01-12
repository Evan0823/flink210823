package com.atguigu.flink.chapter05.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;


/**Source--->从Kafka读取数据(实际生产常用)
 *
 * @author Evan
 * @ClassName Flink01_Source_File
 * @date 2022-01-12 14:47
 */
public class Flink03_Source_kafka {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        props.setProperty("group.id","Flink03_Source_Kafka");
        props.setProperty("auto.reset.offset","latest");

        /*DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>("s1", new SimpleStringSchema(), props));*/
        DataStreamSource<ObjectNode> stream = env
                .addSource(new FlinkKafkaConsumer<>("s1", new JSONKeyValueDeserializationSchema(true), props));
        stream.print();

        env.execute();

    }
}
