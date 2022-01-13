package com.atguigu.flink.chapter05.transform;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**process算子：
 *          在所有的流上都能使用这个算子
 *          从流中可以获取到很多信息（数据本身、上下文、窗口的开始结束时间...）
 *          KeyBy之前之后都能使用
 *
 * @author Evan
 * @ClassName Flink10_Process
 * @date 2022-01-13 9:17
 */
public class Flink11_Process {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //env.setParallelism(1);
        env.setParallelism(2);
        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 10),
                new WaterSensor("sensor_1", 3L, 30),
                new WaterSensor("sensor_1", 2L, 50),
                new WaterSensor("sensor_1", 5L, 50),
                new WaterSensor("sensor_2", 5L, 50),
                new WaterSensor("sensor_2", 5L, 50)
        );

        stream
            .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                int sum = 0; //是一种状态(每个元素进来都能访问到跟元素本身没有关系)，
                                        // 没有让flink帮助我们管理，会有很多问题
                            // 状态和并行度有关(process并行度为2，就会new 2个对象，最后的结果也不是我们想要的)，
                @Override               // 后面会学到状态编程，让flink帮助我们管理
                public void processElement(WaterSensor value, // 元素进来一个 处理一次
                                           Context ctx,
                                           Collector<WaterSensor> out) throws Exception { // 处理后会产生新的流，按照collect方法在新的流中放元素
                    sum += value.getVc();

                    value.setVc(sum);

                    out.collect(value);
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
