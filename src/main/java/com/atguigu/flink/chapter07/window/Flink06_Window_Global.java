package com.atguigu.flink.chapter07.window;

import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**GlobalWindow
 *      默认情况窗口永远不会关闭, 所以process就永远不触发计算
 *      需要指定触发器时才有用
 *      有没有keyBy都行
 *
 * @author Evan
 * @ClassName Flink_Window_Tunbing
 * @date 2022-01-14 11:19
 */
public class Flink06_Window_Global {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env
            .socketTextStream("hadoop102", 9999)
            .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                @Override
                public void flatMap(String value,
                                    Collector<Tuple2<String, Long>> out) throws Exception {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }

                }
            })
            .keyBy(t -> t.f0)
            .window(GlobalWindows.create())
            .trigger(new Trigger<Tuple2<String, Long>, GlobalWindow>() {
                // 每来一个元素触发一次
                int count = 0;
                @Override
                public TriggerResult onElement(Tuple2<String, Long> element,
                                               long timestamp,
                                               GlobalWindow window,
                                               TriggerContext ctx) throws Exception {
                    count++;
                    if(count < 3) {
                        return TriggerResult.CONTINUE;
                    }
                    count = 0;
                    return TriggerResult.FIRE_AND_PURGE; // 触发窗口计算并把结果发送出去
                }

                // 如果是基于处理时间的窗口, 需要实现这个方法
                @Override
                public TriggerResult onProcessingTime(long time,
                                                      GlobalWindow window,
                                                      TriggerContext ctx) throws Exception {
                    return null;
                }

                // 如果是基于事件时间的窗口, 需要实现这个方法
                @Override
                public TriggerResult onEventTime(long time,
                                                 GlobalWindow window,
                                                 TriggerContext ctx) throws Exception {
                    return null;
                }

                // 清空一些状态
                @Override
                public void clear(GlobalWindow window,
                                  TriggerContext ctx) throws Exception {

                }
            })
            .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, GlobalWindow>() {
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<Tuple2<String, Long>> elements,
                                    Collector<String> out) throws Exception {
                    System.out.println("xxxxx");
                    List<Tuple2<String, Long>> list = AtguiguUtil.toList(elements);
                    out.collect(ctx.window() + "  " + list);
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
