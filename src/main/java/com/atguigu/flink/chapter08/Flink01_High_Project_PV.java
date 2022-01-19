package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**每隔半个小时, 统计最近1小时的PV
 *
 * @author Evan
 * @ClassName Flink01_High_Project_PV
 * @date 2022-01-19 14:07
 */
public class Flink01_High_Project_PV {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env
            .readTextFile("input/UserBehavior.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new UserBehavior(
                        Long.valueOf(data[0]),
                        Long.valueOf(data[1]),
                        Integer.valueOf(data[2]),
                        data[3],
                        Long.parseLong(data[4]) * 1000 // 事件时间必须是毫秒, 封装的时候是秒

                );
            })
            .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                            .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                            .withTimestampAssigner((ub, ts) -> ub.getTimestamp())
            )
            .filter(ub -> "pv".equals(ub.getBehavior()))
            .keyBy(UserBehavior::getBehavior)
            .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(30)))
            .aggregate(
                    new AggregateFunction<UserBehavior, Long, Long>() { // 中间是累加器类型
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(UserBehavior value, Long acc) {
                            return acc + 1;
                        }

                        @Override
                        public Long getResult(Long acc) {
                            return acc;
                        }

                        @Override
                        public Long merge(Long a, Long b) {
                            return null;
                        }
                    },
                    // 输入前面聚合的结果、输出、K的类型、窗口的类型
                    new ProcessWindowFunction<Long, String, String, TimeWindow>() {

                        private SimpleDateFormat sdf;

                        @Override
                        public void open(Configuration parameters) throws Exception {
                            // 时间格式化器
                            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        }

                        @Override
                        public void process(String key,
                                            Context ctx,
                                            Iterable<Long> elements,
                                            Collector<String> out) throws Exception {

                            String stt = sdf.format(ctx.window().getStart());
                            String edt = sdf.format(ctx.window().getEnd());

                            Long result = elements.iterator().next();

                            out.collect(stt + " " + edt + " " + result);
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
}
