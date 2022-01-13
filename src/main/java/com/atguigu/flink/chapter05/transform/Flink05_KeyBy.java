package com.atguigu.flink.chapter05.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**KeyBy算子：flink中聚合、开窗等都频繁使用到了
 *          到底是如何选择并行度的？
 * 详见源码：
 *      结论：key经过双重Hash(hashCode、murmurHash)来选择它的并行度
 *
 * 什么值不可以作为KeySelector的Key？
 *      任何类型的数组，大多数为String类型
 * new KeyGroupStreamPartitioner<>(
 *                                 keySelector,
 *                                 StreamGraphGenerator.DEFAULT_LOWER_BOUND_MAX_PARALLELISM)
 *
 * 128
 *
 * 0或1  128  2
 *  KeyGroupRangeAssignment.assignKeyToParallelOperator(
 *                 key, maxParallelism, numberOfChannels);
 *
 *
 * 128  2  [0, 128)
 * computeOperatorIndexForKeyGroup(
 *                 maxParallelism, parallelism, assignToKeyGroup(key, maxParallelism));
 *
 *                   0或1  128
 *             assignToKeyGroup(key, maxParallelism)   [0, 128)
 *                     0或1  128
 *                 computeKeyGroupForKeyHash(key.hashCode(), maxParallelism); [0, 128)
 *
 *                     MathUtils.murmurHash(keyHash) % maxParallelism; // [0, 128)
 *
 *     [0,128) * 2 / 128 =
 *     小于64的值乘以2 小于128 除以128 = 0
 *     大于等于64的值乘以2 大于128 除以128 = 1
 *     keyGroupId * parallelism / maxParallelism;
 *
 * @author Evan
 * @ClassName Flink05_KeyBy
 * @date 2022-01-12 20:03
 */
public class Flink05_KeyBy {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<Integer> stream = env.fromElements(1, 3, 8, 4, 10);

        // 需求：偶数一个组(key相同)，奇数一个组；一个组也就意味着一定在同一个并行度
            // 相同key的元素会分到同一个分区中，一个分区中可以有多重不同的key
        stream
            .keyBy(new KeySelector<Integer, String>() {
                /*@Override
                public Integer getKey(Integer value) throws Exception {
                    return value % 2;
                }*/
                @Override
                public String getKey(Integer value) throws Exception {
                    return value % 2 == 0 ? "偶数" : "奇数";
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
