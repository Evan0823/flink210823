package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**每隔半个小时, 统计最近1小时的PV
 *      ---使用带聚合的状态去实现
 *
 * 注意：要清空状态
 *
 * @author Evan
 * @ClassName Flink01_High_Project_PV
 * @date 2022-01-19 14:07
 */
public class Flink01_High_Project_PV_1 {
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
                .process(new ProcessWindowFunction<UserBehavior, String, String, TimeWindow>() {

                    private AggregatingState<UserBehavior, Long> pvState;
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        pvState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<UserBehavior, Long, Long>(
                                "pvState",
                                new AggregateFunction<UserBehavior, Long, Long>() {
                                    @Override
                                    public Long createAccumulator() {
                                        return 0L;
                                    }

                                    @Override
                                    public Long add(UserBehavior value, Long accumulator) {
                                        return accumulator + 1;
                                    }

                                    @Override
                                    public Long getResult(Long accumulator) {
                                        return accumulator;
                                    }

                                    @Override
                                    public Long merge(Long a, Long b) {
                                        return null;
                                    }
                                },
                                Long.class
                        ));

                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<UserBehavior> elements,
                                        Collector<String> out) throws Exception {

                        pvState.clear(); // 键控状态只和key有关, 和窗口没有关系, 所以同一个key的所有窗口共用一个状态
                        for (UserBehavior element : elements) {
                            pvState.add(element);
                        }
                        String stt = sdf.format(ctx.window().getStart());
                        String edt = sdf.format(ctx.window().getEnd());

                        Long result = pvState.get();
                        out.collect(stt + "  " + edt + "  " + result);
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
