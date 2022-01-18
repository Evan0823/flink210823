package com.atguigu.flink.chapter07.timer;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**定时器---基于事件时间
 *
 *      如果去掉keyBey会报错，说明定时器只能在keyBy之后的流上
 *
 * @author Evan
 * @ClassName Flink01_Timer_1
 * @date 2022-01-17 14:54
 */
public class Flink02_Timer_3 {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String line) throws Exception {
                        String[] data = line.split(",");
                        return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((ws, ts) -> ws.getTs())
                )
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    long time;

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        System.out.println("触发定时器");

                        ctx.timerService().registerEventTimeTimer(value.getTs() + 5000);
                    }

                    @Override
                    public void onTimer(long timestamp,
                                        OnTimerContext ctx,
                                        Collector<String> out) throws Exception {
                        System.out.println("触发:" + timestamp);

                        out.collect("abc");
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
