package com.atguigu.flink.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**批处理
 *
 * 依赖里"Provided" scope： 表示这个依赖只编译不参与运行，防止打包后依赖冲突，但在IDEA中需要运行怎么处理？
 *                      --->勾选上include dependencies with "Provided" scope
 * FlatMapFunction<String, String>：输入是一行String类型，输出是一个个单词String类型
 * Tuple2：元祖
 *
 *
 *
 *
 * WordCount处理
 * flink：
 * 1. 创建批处理的执行环境
 * 2. 从数据源(source)读取数据
 * 3. 对数据做各种转换
 * 4. 输出(sink)
 *
 *
 * spark:
 * 1. 创建上下文
 * 2. 通过上下文从数据源读取数据, 得到rdd
 *
 * 3. 对rdd做各种转换操作
 *
 * 4. 执行行动算子
 *
 * 5. 关闭上下文
 * @author Evan
 * @ClassName Flink01_BatchWordCount
 * @date 2022-01-10 19:06
 */
public class Flink01_BatchWordCount {
    public static void main(String[] args) throws Exception {

        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 从数据源(source)取数据
        DataSource<String> wordDS = env.readTextFile("input/words.txt");
        FlatMapOperator<String, String> wordFMO = wordDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(word);
                }
            }
        });
        // 3. 使用map将数据转换格式  word -> (word,1L)
        MapOperator<String, Tuple2<String, Long>> wordTuple = wordFMO.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return Tuple2.of(word, 1L);
            }
        });
        // 4. 对相同key 的数据进行聚合操作
        UnsortedGrouping<Tuple2<String, Long>> wordUG = wordTuple.groupBy(0);
        // 5. 对分组内的数据进行求和
        AggregateOperator<Tuple2<String, Long>> wordAgg = wordUG.sum(1);
        // 6. 输出(sink)
        wordAgg.print();
    }
}

