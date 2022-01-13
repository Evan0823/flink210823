package com.atguigu.flink.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;

/**es
 *  ---有界流、无界流演示
 *
 * index    数据库
 * type      表
 *          5.x之前  一个index可以有多个type
 *          6.x开始  一个index只能有一个type
 *          7.x开始  去掉了type
 * doc      行
 *
 * @author Evan
 * @ClassName Flink03_ES
 * @date 2022-01-13 22:14
 */
public class Flink03_ES {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        /*DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 10),
                new WaterSensor("sensor_1", 3L, 30),
                new WaterSensor("sensor_1", 2L, 50),
                new WaterSensor("sensor_1", 5L, 50),
                new WaterSensor("sensor_2", 5L, 50),
                new WaterSensor("sensor_2", 5L, 50)
        );*/

        SingleOutputStreamOperator<WaterSensor> stream = env
            .socketTextStream("hadoop102", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
            });

        ArrayList<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("hadoop102", 9200));
        hosts.add(new HttpHost("hadoop103", 9200));
        hosts.add(new HttpHost("hadoop104", 9200));

        ElasticsearchSink.Builder<WaterSensor> builder = new ElasticsearchSink.Builder<WaterSensor>(
                hosts,
                new ElasticsearchSinkFunction<WaterSensor>() {
                    @Override
                    public void process(WaterSensor element,
                                        RuntimeContext ctx,
                                        RequestIndexer indexer) {
                        // 实现这个方法, 来完成向es写数据
                        IndexRequest request = Requests
                                .indexRequest()
                                .index("sensor")
                                .type("_doc")
                                .id(element.getId())
                                .source(JSON.toJSON(element), XContentType.JSON);

                        indexer.add(request);
                    }
                }
        );

        // ES往外写数据时是批处理，如果处理的是无界流，以下三个参数至少添加一个，具体跟实际情况来定
        // 设置刷数据到es的间隔(默认没有值,代表不会刷新)
        builder.setBulkFlushInterval(1000);
        // 来一条数据刷新一次(默认没有值)
        builder.setBulkFlushMaxActions(1);
        // 数据积累到多大就刷新一次
        builder.setBulkFlushMaxSizeMb(10);

        stream
                .keyBy(WaterSensor::getId)
                .sum("vc")
                .addSink(builder.build());
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
