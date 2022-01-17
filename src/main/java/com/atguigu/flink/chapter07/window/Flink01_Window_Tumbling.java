package com.atguigu.flink.chapter07.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

/**基于时间的窗口---滚动窗口
 *      统计5s内的WordCount变型
 *
 *      滚动窗口(只设置窗口长度)：
 *      滑动窗口(窗口长度、滑动步长；有间隙、重叠)：如滑动窗口长度10分钟, 滑动步长5分钟,则,每5分钟会得到一个包含最近10分钟的数据
 *      会话窗口(固定的Gap,间隙超过这个时间,就会属于另一个Session)：不同的key都有自己的窗口
 *
 * 获取窗口的开始、结束时间
 * 正则表达式：生产中会用到
 *
 * @author Evan
 * @ClassName Flink_Window_Tunbing
 * @date 2022-01-14 11:19
 */
public class Flink01_Window_Tumbling {
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
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 调用的一个静态方法
            // 处理窗口的函数<IN, OUT, KEY, W extends Window>
            .process(new ProcessWindowFunction<Tuple2<String,Long>, String, String, TimeWindow>(){
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<Tuple2<String, Long>> elements, // 窗口内所有的元素
                                    Collector<String> out) throws Exception {
                    TimeWindow w = ctx.window(); // 获取到一个窗口对象
                    Date start = new Date(w.getStart());
                    Date end = new Date(w.getEnd());

                    int count = 0;
                    for (Tuple2<String, Long> element : elements) {
                        count++; // 窗口长度
                    }
                    out.collect(key + "," + start + "," + end + "," + count);
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
