package com.atguigu.flink.chapter07.window;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author Evan
 * @ClassName Flink08_Window_Aggregate
 * @date 2022-01-14 15:19
 */
public class Flink08_Window_Aggregate {
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
                })
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                // AggregateFunction<IN, ACC, OUT> 输入、累加器(保存中间结果)、输出
                .aggregate(
                        new AggregateFunction<WaterSensor, Avg, Double>() {
                            // 创建一个累加器, 并返回这个累加器(只触发一次，当某个key的某个窗口中的第一个元素来的时候触发)
                            @Override
                            public Avg createAccumulator() {
                                // soutm：打印出方法名和类名，便于查看执行的先后顺序
                                System.out.println("Flink08_Window_Aggregate.createAccumulator");
                                return new Avg();
                            }

                            // 真的聚合方法. 根据来的元素, 更新累加器中的属性的值(每来一个元素触发一次)
                            @Override
                            public Avg add(WaterSensor value, Avg acc) {
                                System.out.println("Flink08_Window_Aggregate.add");
                                acc.vcSum += value.getVc();
                                acc.count++;
                                return acc;
                            }

                            // 返回聚合的最终结果(窗口关闭, 触发一次)
                            @Override
                            public Double getResult(Avg acc) {
                                System.out.println("Flink08_Window_Aggregate.getResult");
//                                return acc.vcSum * 1.0 / acc.count;
                                return acc.avg(); // 更具有面向对象的思想
                            }

                            // 合并累加器. 这个方法只有Session窗口才会触发, 其他窗口不会触发, 所以可以不用实现
                            @Override
                            public Avg merge(Avg a, Avg b) {
                                System.out.println("Flink08_Window_Aggregate.merge");
                                a.vcSum += b.vcSum;
                                a.count += b.count;
                                return a;
                            }
                        },
                        // 如果还想看窗口的信息
                        new ProcessWindowFunction<Double, String, String, TimeWindow>() {
                            @Override
                            public void process(String key,
                                                Context ctx,
                                                Iterable<Double> elements,
                                                Collector<String> out) throws Exception {
                                Double result = elements.iterator().next();
                                out.collect(key + "  " + ctx.window() + "  " + result);
                            }
                        }
                )
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 累加器（就不用元祖使用一般的JavaBean,Java中元祖不好用）
    public static class Avg {
        public Integer vcSum = 0;
        public Long count = 0L;

        public Double avg() {
            return this.vcSum * 1.0 / this.count;
        }
    }
}
