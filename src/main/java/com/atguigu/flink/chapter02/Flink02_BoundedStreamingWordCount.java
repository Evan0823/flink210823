package com.atguigu.flink.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**流式处理（有边界的）
 *
 * flink流处理分组算子：keyBy
 *
 * @author Evan
 * @ClassName Flink02_BoundedStreamingWordcount
 * @date 2022-01-10 19:51
 */
public class Flink02_BoundedStreamingWordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 并行度

       env
           .readTextFile("input/words.txt")
           .flatMap(new FlatMapFunction<String, String>() {
               @Override
               public void flatMap(String line,
                                   Collector<String> out) throws Exception {
                   for (String word: line.split(" ")) {
                       out.collect(word);
                   }

               }
           })
           .map(new MapFunction<String, Tuple2<String, Long>>() {
               @Override
               public Tuple2<String, Long> map(String word) throws Exception {
                   return Tuple2.of(word,1L);
               }
           })
            .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                @Override
                public String getKey(Tuple2<String, Long> t) throws Exception {
                    return t.f0; // t._1
                }
            })
           .sum(1)
           .print();

       env.execute(); //执行流式环境
    }
}
