package com.atguigu.flink.chapter07.timer;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**定时器---基于处理时间
 *
 * @author Evan
 * @ClassName Flink01_Timer_1
 * @date 2022-01-17 14:54
 */
public class Flink01_Timer_1 {
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
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    if (value.getVc() > 20) {
                        // 当水位超过20, 然后5s之后发出红色预警
                        long time = System.currentTimeMillis() + 5000;
                        System.out.println("定义定时器: " + time);
                        ctx.timerService().registerProcessingTimeTimer(time); // 可以用一个子线程吗？不行,主线程蹦了一起蹦,所以用一个定时器
                    }
                }

                // 当定时器触发的时候, 会自动回调这个方法
                @Override
                public void onTimer(long timestamp, // 定时器的时间
                                    OnTimerContext ctx,
                                    Collector<String> out) throws Exception {
                    System.out.println("触发:" + timestamp);

                    // 发出预警
                    out.collect(ctx.getCurrentKey() + "水位超过了20 ,发出预警...");
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
