package com.atguigu.flink.chapter07.window;

import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**基于时间的窗口
 *      滚动窗口
 *      滑动窗口：如滑动窗口长度3个元素,滑动步长2个元素,则每来两个元素触发一次,一个窗口最多3个元素
 *      获取窗口的信息(是一个全局窗口)
 *
 * @author Evan
 * @ClassName Flink_Window_Tunbing
 * @date 2022-01-14 11:19
 */
public class Flink04_Window_Count {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

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
//            .countWindow(3)
            // 每来两个元素触发一次,一个窗口最多3个元素
            .countWindow(3, 2)
            .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, GlobalWindow>() {
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<Tuple2<String, Long>> elements,
                                    Collector<String> out) throws Exception {
                    List<Tuple2<String, Long>> list = AtguiguUtil.toList(elements);
                    out.collect(ctx.window() + "  " + list.toString());
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
