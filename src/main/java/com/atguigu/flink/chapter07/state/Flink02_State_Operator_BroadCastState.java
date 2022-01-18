package com.atguigu.flink.chapter07.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**算子状态---广播状态
 *
 *      广播状态存储值就是以kv的形式存储,广播状态本质是一个map
 *
 *      使用广播状态:
 *          需要两个流, 一个流一般是数据流, 另外一个流是配置流
 *          使用配置流的配置信息来控制(动态控制)数据流中的业务逻辑
 *
 * @author Evan
 * @ClassName Flink01_State_Operator
 * @date 2022-01-18 9:20
 */
public class Flink02_State_Operator_BroadCastState {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env.enableCheckpointing(1000); // 开启checkpoint周期

        MapStateDescriptor<String, String> bcStateDesc = new MapStateDescriptor<>("bcState", String.class, String.class);

        DataStreamSource<String> dataTextStream = env.socketTextStream("hadoop102", 8888);
        DataStreamSource<String> controlStream = env.socketTextStream("hadoop102", 9999);

        // 1. 把控制流做成广播流  广播状态存储值就是以kv的形式存储, 本质是一个map
        BroadcastStream<String> bcStream = controlStream.broadcast(bcStateDesc);
        // 2. 数据流和广播流进行connect
        // 3. 处理
        dataTextStream
                .connect(bcStream)
                // <String, String, String> 数据流、广播流、结果
                .process(new BroadcastProcessFunction<String, String, String>() {
                    // 处理数据流中的元素
                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        // 读取广播流中的元素, 根据值决定代码怎么写.
                        ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(bcStateDesc); // 得到一个广播状态(只能读)
                        String aSwitch = state.get("switch");

                        if ("1".equals(aSwitch)) {
                            out.collect("使用 1 号逻辑处理代码....");
                        } else if ("2".equals(aSwitch)) {
                            out.collect("使用 2 号逻辑处理代码....");
                        } else {
                            out.collect("使用 default 号逻辑处理代码....");
                        }
                    }

                    // 处理广播流中的元素
                    @Override
                    public void processBroadcastElement(String value,
                                                        Context ctx,
                                                        Collector<String> out) throws Exception {
                        // 把广播流中的元素存入到广播状态, 将来, 在这个app中的任何一个并行度中都可以读取到这个状态中的数据
                        BroadcastState<String, String> state = ctx.getBroadcastState(bcStateDesc); // 得到一个广播状态(既能读又能写)
                        state.put("switch", value);
                    }
                })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class MyFlatMapFuntion implements FlatMapFunction<String, String>, CheckpointedFunction {
        ArrayList<String> words = new ArrayList<>(); // 将集合放到了外面,则每个并行度一个list集合
        private ListState<String> wordsState;

        // 把每一行字符串, 切成一个个的单词, 放入到一个list集合中, 就是我们需要保存的状态
        @Override
        public void flatMap(String line, Collector<String> out) throws Exception {
            // 手动抛出一个异常, 程序因为开了checkpoint, 会自动重启
            if (line.contains("x")) {
                throw new RuntimeException("手动抛异常, flink自动重启");
            }

            for (String word : line.split(",")) {
                words.add(word);
            }
            out.collect(words.toString());
        }

        // 给状态做快照, 把状态持久化, 将来恢复的时候可以从持久化的位置(由专门的组件状态后端决定,默认持久化在JobManager内存)实现恢复状态中的数据
        // 周期性的保存状态, 由checkpoint的周期决定. 每个并行执行一次
        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            //System.out.println("MyFlatMapFuntion.snapshotState");

            /**
             * 需要获取到下面算子的状态(由于通过上下文拿不到,只有将其提升为全局变量ctrl+alt+f)
             */
            wordsState.update(words); // 用新集合中的元素, 覆盖状态中的元素

            /*wordsState.clear();
            wordsState.addAll(words);*/ // 追加(追加前清空达到更新作用)
        }

        // 初始化状态: 当程序启动的时候, 会对状态进行恢复
        // 当程序启动的时候执行, 有几个并行度就执行几次, 每个并行度都要恢复自己的状态
        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            System.out.println("MyFlatMapFuntion.initializeState");

            // 从checkpoint中获取保存的算子状态, 每个并行度一份 (恢复)
            // ListStateDescriptor联合列表描述器
            wordsState = ctx.getOperatorStateStore().getUnionListState(new ListStateDescriptor<String>("ws", String.class));

            Iterable<String> iterable = wordsState.get();
            for (String word : iterable) {
                //System.out.println("xxxxxx");

                words.add(word);
            }
        }
    }
}
