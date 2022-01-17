package com.atguigu.flink.chapter07.window;

import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**keyBy之前使用窗口
 *      注意：窗口的使用一般在keyBy之后(按照key来分的每个key都有自己的窗口)，之前使用意义不大(因为并行度只会为1)。
 *
 * 		soketSource是并行度为1的算子
 *
 * @author Evan
 * @ClassName Flink_Window_Tunbing
 * @date 2022-01-14 11:19
 */
public class Flink05_Window_Count_No_Key {
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
            // 所有的数据进入同一个窗口. 窗口处理函数的并行度只能是1
            .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .process(new ProcessAllWindowFunction<Tuple2<String, Long>, String, TimeWindow>() {
                @Override
                public void process(Context ctx,
                                    Iterable<Tuple2<String, Long>> elements,
                                    Collector<String> out) throws Exception {
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
