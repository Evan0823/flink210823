package com.atguigu.flink.chapter07.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic.EXACTLY_ONCE;

/**端到端的状态一致
 *
 * 难点：
 * 2、测试---自定义一个sink手动抛异常, 导致整个事务关闭失败, 事务回滚但数据一旦写入Kafka就不能删除,
 *          由于开启了checkpoint, 会无限次重启, 出现很多x_1写入Kafka, 就出现了违背严格一次的现象
 *      Kafka/bin/kafka-console-consumer.sh 查看参数
 *      消费者隔离级别
 *          isolation.level(read_committed)：只读取已提交事务的数据
 *          isolation.level(read_uncommitted)：读取已提交的数据和未关闭事务时未提交的数据(默认)
 *      为什么读取到的速度不一样？
 *      因为第二种情况可以读取未提的(数据一写入Kafka就能读走), 读取已提交的(只有当事务关闭才能读走),
 *          第一情况想要读取别人的已提交的只有等第二个它自己的barrier事务结束后才能读走 (barrier对齐---flink严格一次)
 *      所以说企业中就会涉及到checkpoint周期与实效性的平衡！！！(读取已提交的数据时)
 *      (读取未提交的数据时)不会存在以上问题, 但会存在一个读取重复数据, 一个永远读不到标记为未提交的数据的问题
 *
 *
 * 1、报错---Kafka事务的超时时间超过了服务端的时间限制
 *      生产者写数据到Kafka, 写数据会有一个事务, Kafka broker服务器对事务的开始时间到结束时间有最大限制15min要求
 *          当生产者开启的事务时间60min大于Kafka broker的时间限制, 则会报错
 *            服务端参数transaction.max.timeout.ms
 *            生产者参数transaction.timeout.ms
 *
 *
 * 打包在shell上演示
 *      生产者 producer s1
 *      消费者 bin/kafka-console-consumer.sh --bootstrap-server hadoop102:9092 --topic s2 --isolation-level read_committed
 *      启动程序
 *      flink-yarn/bin/flink run -d -c com.atguigu.flink.chapter07.state.Flink09_Kafka_Flink_Kafka -s hdfs://hadoop102:8020/ck20/51e3032ac93d2319a04935a70cee33e3/chk-65 flink210823-1.0-SNAPSHOT.jar
 *      先演示一些数据, 再去页面关闭Job, 再生产一个数据
 *      flink-yarn/bin/flink run -d -c com.atguigu.flink.chapter07.state.Flink09_Kafka_Flink_Kafka -s hdfs://hadoop102:8020/ck20/51e3032ac93d2319a04935a70cee33e3/chk-65 flink210823-1.0-SNAPSHOT.jar
 *      由于加了命令, 再次启动一个程序后, 会从指定的位置开始消费
 *
 * checkpoint的强大！！！
 *
 * @author Evan
 * @ClassName Flink09_Kafka_Flink_Kafka
 * @date 2022-01-19 11:53
 */
public class Flink09_Kafka_Flink_Kafka {
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.enableCheckpointing(3000);// checkpoint周期
        env.setStateBackend(new HashMapStateBackend()); // 设置状态后端
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck20"); // 存储checkpoint状态

        /**
         * checkpoint常用配置
         */
        // 设置checkpoint的语义 (至少一次(barrier不对齐)或者严格变一次(barrier对齐))
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 同时允许最大checkpoint的数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500); // 两个checkpoint最小间隔时间
        // 当取消job是否把checkpoint数据继续保留下来
        env.getCheckpointConfig().enableExternalizedCheckpoints(RETAIN_ON_CANCELLATION);
        // checkpoint超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        /**
         * Kafka消费者常用配置
         */
        Properties sourceProps = new Properties();
        sourceProps.setProperty("bootstrap.servers", "hadoop102:9092");
        sourceProps.setProperty("group.id", "Flink09_Kafka_Flink_Kafka2");
        sourceProps.setProperty("auto.reset.offset", "latest"); // 如果没有消费记录, 从最新的位置开始消费
        sourceProps.setProperty("isolation.level", "read_committed"); // 只读取别人已经提交的数据, 防止重复消费

        /**
         * Kafka生产者常用配置
         */
        Properties sinkProps = new Properties();
        sinkProps.setProperty("bootstrap.servers", "hadoop102:9092");
        // Kafka broker时间限制15min, 不允许生产者最大时间超过它
            // 服务端参数transaction.max.timeout.ms
            // 生产者参数transaction.timeout.ms
        sinkProps.setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "");


        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = env
            .addSource(new FlinkKafkaConsumer<String>("s1", new SimpleStringSchema(), sourceProps))
            .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                @Override
                public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                    for (String word : value.split(" ")) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }
            })
            .keyBy(t -> t.f0)
            .sum(1);

        // 将数据写入Kafka (设置sink的严格一次语义,开启事务两阶段提交) // 默认是至少一次不开启事务的
        resultStream.addSink(new FlinkKafkaProducer<Tuple2<String, Long>>(
                "default",
                new KafkaSerializationSchema<Tuple2<String, Long>>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Long> element, @Nullable Long aLong) {
                        // 第二个参数, 把Tuple拼成一个字节数组
                        return new ProducerRecord<>("s2", (element.f0 + "_" + element.f1).getBytes(StandardCharsets.UTF_8));
                    }
                },
                sinkProps,
                EXACTLY_ONCE
        ));

        resultStream.addSink(new SinkFunction<Tuple2<String, Long>>() {
            @Override
            public void invoke(Tuple2<String, Long> value,
                               Context context) throws Exception {
                if (value.f0.contains("x")) {
                    throw new RuntimeException("碰到了x, 抛异常");
                }
            }
        });

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
