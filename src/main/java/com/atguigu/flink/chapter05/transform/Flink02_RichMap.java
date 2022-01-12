package com.atguigu.flink.chapter05.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**任何一个算子在传一个函数作为参数，一个接口XxxFunction，一个类RichXxxFunction
 * RichMapFunction()：富有版本，有两个关键的方法open()、close()，作用：连接外部数据库时，每个并行度执行一次
 * （与spark中mapPartition/foreachPartition类似）
 *
 * @author Evan
 * @ClassName Flink02_RichMap
 * @date 2022-01-12 20:02
 */
public class Flink02_RichMap {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<Integer> stream = env.fromElements(1, 3, 8, 4, 10);

        stream
            .map(new RichMapFunction<Integer, Integer>() {
                // 在这里建立到外部系统的连接
                // 执行只和并行度有关系（通过改变并行度来测试）
                @Override
                public void open(Configuration parameters) throws Exception {
                    System.out.println("open...");
                }

                @Override
                public void close() throws Exception {
                    // 关闭连接
                    System.out.println("close...");
                }

                @Override
                public Integer map(Integer value) throws Exception {
                    return value * value;
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
