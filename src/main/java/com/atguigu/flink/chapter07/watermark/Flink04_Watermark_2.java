package com.atguigu.flink.chapter07.watermark;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

/**允许迟到数据
 *
 * 解决迟到数据:
 * 1. 事件时间+水印
 * 2. 允许迟到
 *     窗口的关闭时间到了, 先对窗口内的数据进行计算, 窗口暂时不管,
 *     在允许范围之内, 进来的数据, 会重新触发这个窗口的计算. 等到超过允许范围之后, 窗口才真正的关闭
 * 3. 如果窗口真正的关闭
 *     还有迟到数据, flink会把数据放入到一个侧输出流中
 *
 * @author Evan
 * @ClassName Flink04_Watermark_2
 * @date 2022-01-17 14:09
 */
public class Flink04_Watermark_2 {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102",9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String line) throws Exception {
                        String[] data = line.split(",");
                        return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                    }
                }).setParallelism(2)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                // 乱序程度(生产中根据经验来定)
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                // 提取出事件时间
                                .withTimestampAssigner( (element, ts) -> element.getTs()) //lambda
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //
                .allowedLateness(Time.seconds(3))
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
}
