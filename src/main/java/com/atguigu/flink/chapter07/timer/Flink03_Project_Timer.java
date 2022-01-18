package com.atguigu.flink.chapter07.timer;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 需求：监控水位传感器的水位值，如果水位值在5s之内(event time)连续上升，则报警。
 *
 *      无论哪种窗口都实现不了，用定时器实现
 *
 * @author Evan
 * @ClassName Flink01_Timer_1
 * @date 2022-01-17 14:54
 */
public class Flink03_Project_Timer {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
            .socketTextStream("hadoop162", 9999)
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
                boolean isFirst = true;
                long time;
                int lastVc;

                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    if (isFirst) {
                        isFirst = false;
                        time = value.getTs() + 5000;
                        System.out.println("第一条数过来, 注册定时器:" + time);
                        ctx.timerService().registerEventTimeTimer(time);
                    } else {
                        if (value.getVc() > lastVc) {
                            System.out.println("水位上升, 无需操作...");
                        } else {
                            System.out.println("水位没有上升, 注销上次注册的定时器");
                            ctx.timerService().deleteEventTimeTimer(time);
                            time = value.getTs() + 5000;
                            System.out.println("再注册新的定时器:" + time);
                            ctx.timerService().registerEventTimeTimer(time);
                        }
                    }

                    // 处理完所有逻辑之后,把当前的水位保存到上一个水位,等下一次使用
                    lastVc = value.getVc();
                }

                @Override
                public void onTimer(long timestamp,
                                    OnTimerContext ctx,
                                    Collector<String> out) throws Exception {
                    out.collect("连续5s上升, 发出预警...");

                    isFirst = true;
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
