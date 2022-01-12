package com.atguigu.flink.chapter04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * taskmanager.numberOfTaskSlots: 1
 *     设置每个TaskManager的slot的个数
 * 如何设置并行度:
 * 1.在flink的配置文件中
 *     parallelism.default: 1  设置所有job的所有算子的默认并行度
 *
 * 2.在提交job的设置
 *    flink run  -p 3  ...
 *
 * 3.通过执行环境设置
 *      env.setParallelism(1);
 *
 * 4.单独给某个算子设置并行度
 *
 *
 *
 * 操作链的优化:
 * 1 .startNewChain()
 *     开启一个新的链. 当前算子不会与前面的算子优化到一起
 *
 * 2 .disableChaining()
 *     禁用当前算子与其他算子进行操作链的优化
 *
 * 3 .env.disableOperatorChaining();
 *     全局禁用操作链的优化
 *
 *
 * @author Evan
 * @ClassName Flink03_UnBoundedStreamingWordcount
 * @date 2022-01-10 19:51
 */
public class Flink01_UnBoundedStreamingWordCount {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000); // 设置webUI通信端口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 9999)
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
                        return t.f0;
                    }
                })
                .sum(1)
                .print();

        env.execute();
    }
}
