package com.atguigu.flink.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 五大数据类型：
 * string
 * list
 * set
 * hash
 * zset
 *
 * 不同的数据类型写入Redis的方法也不同
 * 返回命令描述符  set hset sadd lpush
 *
 * @author Evan
 * @ClassName Flink02_Redis
 * @date 2022-01-13 11:34
 */
public class Flink02_Redis_String {
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

        FlinkJedisConfigBase config = new FlinkJedisPoolConfig.Builder()
            .setHost("hadoop102")
            .setPort(6379)
            .setMaxTotal(500)
            .setMinIdle(100)
            .setDatabase(0)
            .setTimeout(10 * 1000)
            .build();

        // string
        stream
            .keyBy(WaterSensor::getId) // 方法的引用
            .sum("vc")
            .addSink(new RedisSink<>(
                config,
                new RedisMapper<WaterSensor>() {
                    // 返回命令描述符  set hset sadd lpush
                    @Override
                    public RedisCommandDescription getCommandDescription() {
                        return new RedisCommandDescription(RedisCommand.SET, "abc"); // 第二个参数只对hash和zset有效
                    }

                    @Override
                    public String getKeyFromData(WaterSensor data) {
                        return data.getId(); // 用sensor id作为key
                    }

                    @Override
                    public String getValueFromData(WaterSensor data) {
                        return JSON.toJSONString(data);
                    }
                }));
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
