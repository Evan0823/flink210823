package com.atguigu.flink.chapter07.watermark;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**侧输出流的作用2：流的切分 (此时与时间语义没有关系了)
 *
 * @author Evan
 * @ClassName Flink05_Watermark_3
 * @date 2022-01-17 14:09
 */
public class Flink06_Split_Sideout {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensor1Stream = env
                .socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String line) throws Exception {
                        String[] data = line.split(",");
                        return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                    }
                })
                // sensor_1  sensor_2 分别单独在各自的流中, 其他的所有sensor在一个流
                .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<WaterSensor> out) throws Exception {
                        if ("sensor_1".equals(value.getId())) {
                            out.collect(value);

                        } else if ("sensor_2".equals(value.getId())) {
                            ctx.output(new OutputTag<WaterSensor>("s2") {}, value);

                        } else {
                            ctx.output(new OutputTag<WaterSensor>("other") {}, value);

                        }
                    }
                });

        sensor1Stream.print("s1");
        sensor1Stream.getSideOutput(new OutputTag<WaterSensor>("s2") {}).print("s2");
        sensor1Stream.getSideOutput(new OutputTag<WaterSensor>("other") {}).print("other");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
