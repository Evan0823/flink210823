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
 *      难点：如果输入ts=1000，注册一个定时器6000(1000+5000)，当水印大于等于6000时(当再次输入ts=9000,水印=9001-3000-1=6000)触发定时器
 *
 *      删除定时器：输入注册定时器，再输入另id=2,vc=10会删除定时器，删除的不是前面注册的那个
 *      （说明定时器与key有关）
 *
 * @author Evan
 * @ClassName Flink01_Timer_1
 * @date 2022-01-17 14:54
 */
public class Flink02_Timer_2 {
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
                public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                    if (value.getVc() > 20) { // 当水位超过20,注册定时器(代表然后5s之后发出红色预警)
                        time = value.getTs() + 5000;
                        System.out.println("定义定时器:" + time);
                        ctx.timerService().registerEventTimeTimer(time);
                    } else if (value.getVc() == 10) {
                        System.out.println("删除定时器:" + time); // 当水位为10时删除定时器,上一次的时间
                        ctx.timerService().deleteEventTimeTimer(time);
                    }
                }

                // 当定时器触发的时候, 会自动回调这个方法
                @Override
                public void onTimer(long timestamp,
                                    OnTimerContext ctx,
                                    Collector<String> out) throws Exception {
                    System.out.println("触发:" + timestamp);

                    out.collect(ctx.getCurrentKey() + " 水位超过了20 ,发出预警...");
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
