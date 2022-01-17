package com.atguigu.flink.chapter07.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**Window Function：
 *            增量聚合
 *            全窗口函数
 *
 *      sum max min maxBy minBy(增量聚合)
 *
 *      Reduce(增量聚合)：输入输出一致(个数类型)
 *      Aggregate(增量聚合)： 输入、累加器(保存中间结果)、输出(输入和输出结果是不一样的)
 *      ---前面两个效率比较高，但只能做一些简单的聚合
 *
 *      Process(全量函数/全窗口函数)
 *      ---如果需要求窗口内Top，则可以用Process
 *
 *
 *      位置窗口算子之后
 *
 * @author Evan
 * @ClassName Flink07_Window_Count_Reduce
 * @date 2022-01-14 15:18
 */
public class Flink07_Window_Reduce {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
            .socketTextStream("hadoop102",9999)
            .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
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
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(
                    new ReduceFunction<Tuple2<String, Long>>() {
                        @Override
                        public Tuple2<String, Long> reduce(Tuple2<String, Long> value1,
                                                           Tuple2<String, Long> value2) throws Exception {
                            System.out.println("xxxx");
                            return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                        }
                },
                    new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                        @Override
                        public void process(String key,
                                            Context ctx,
                                            Iterable<Tuple2<String, Long>> elements, // 有且仅有一个值, 就是reduce聚合的最终的结果
                                            Collector<String> out) throws Exception {
                            System.out.println("yyyyyy");
                            Tuple2<String, Long> t = elements.iterator().next(); // 取出前面聚合的最终结果
                            out.collect(key + "  " + ctx.window() + "  " + t);
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
