package com.atguigu.flink.chapter07.watermark;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**水印---升序流(有序流,乱序程度为0)中的水印
 *
 * @author Evan
 * @ClassName Flink02_WaterMark
 * @date 2022-01-17 11:14
 */
public class Flink02_Watermark_Coustom {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(5000);

        env
            .socketTextStream("hadoop102",9999)
            .map(new MapFunction<String, WaterSensor>() {
                @Override
                public WaterSensor map(String line) throws Exception {
                    String[] data = line.split(",");
                    return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                }
            })
            .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                            .forGenerator(new WatermarkStrategy<WaterSensor>() {
                                @Override
                                public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                    return new MyWMGenerator();
                                }
                            })
                            .withTimestampAssigner( (ws,ts) -> ws.getTs())
            )
            .keyBy(WaterSensor::getId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<WaterSensor> elements,
                                    Collector<String> out) throws Exception {
                    List<WaterSensor> list = AtguiguUtil.toList(elements);
                    out.collect(key + " " + ctx.window() + " " + list);
                }
            })
            .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static class MyWMGenerator implements WatermarkGenerator<WaterSensor>{

        // 最大时间戳
        long maxTs = Long.MIN_VALUE + 3000 + 1;

        // 流中每来一个元素就执行一次
        // 如果在这里生产和发射水印, 就是打点式的水印
        @Override
        public void onEvent(WaterSensor event, long ts, WatermarkOutput output) {
            System.out.println("MyWMGenerator.onEvent");

            // 最新的最大时间戳
            maxTs = Math.max(maxTs, ts);

//            output.emitWatermark(new Watermark(maxTs - 3000 - 1));
        }

        // 周期的执行. 默认是每隔200ms执行一次
        // 如果水印在这里生产, 发射出去, 则就是周期的水印
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("MyWMGenerator.onPeriodicEmit");

            output.emitWatermark(new Watermark(maxTs - 3000 - 1));
        }
    }
}
