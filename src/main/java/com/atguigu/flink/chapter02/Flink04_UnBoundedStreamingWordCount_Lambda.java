package com.atguigu.flink.chapter02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**流式处理（无边界的）
 *
 * 什么时候匿名内部类的传递可以用lambda表达式式替换:
 *      如果接受的接口只有一个抽象方法(default方法没有限制), 可以替换
 *          拉姆达表达式
 *          是一种函数式编程  1.8 新增的功能
 *
 * keyBy这个地方代码比较简洁,flatMap,map转换过去存在泛型擦除,则需要用到returns(Types.XXX)明确指出类型
 *
 *
 * @author Evan
 * @ClassName Flink03_UnBoundedStreamingWordcount
 * @date 2022-01-10 19:51
 */
public class Flink04_UnBoundedStreamingWordCount_Lambda {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 9999)
                .flatMap( (String line, Collector<String> out) -> {
                    for (String word: line.split(" ")) {
                        out.collect(word);
                    }
                }).returns(Types.STRING)
                .map( word -> Tuple2.of(word,1L)).returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy( t -> t.f0)
                .sum(1)
                .print();

        env.execute(); //执行流式环境
    }
}
