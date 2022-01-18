package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**键控状态---单值的键控状态
 *
 * 需求：检测传感器的水位值，如果连续的两个水位值超过10，就输出报警。
 *      （思路：需要看上一次的水为值）
 *      （如果使用普通变量保存水位值则没有这种效果，不会保存上一次的状态）
 *
 * 在process中或map/flatMap的富有版本(有open方法)等所有算子中都可以使用。
 *        注意：获取状态一定要放入open方法里，放入构造器里/外面都是不行的
 *              （open方法和并行度关联，在程序启动每个并行度都需要初始化状态，放入open方法在并行度里一定会执行只需初始化一次就能保证每个并行度都有）
 *              （对应close方法在程序结束时候启动，会释放一定资源）
 *
 *      ctrl+alt+f提升为成员变量
 *
 *      5种支持的数据类型：
 *         ValueState
 *         ListState
 *         ReducingState
 *         AggregatingState
 *         MapState
 *
 *
 * @author Evan
 * @ClassName Flink03_State_Keyed_Value
 * @date 2022-01-18 11:15
 */
public class Flink03_State_Keyed_Value {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.enableCheckpointing(1000);// checkpoint周期

        env
            .socketTextStream("hadoop102", 9999)
            .map(new MapFunction<String, WaterSensor>() {
                @Override
                public WaterSensor map(String line) throws Exception {
                    String[] data = line.split(",");
                    return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                }
            })
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                private ValueState<Integer> lastVcState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    /**
                     * 在open方法中获取运行时上下文，然后再获取单值状态
                     */
                    lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Integer.class)); // 存水位
                }

                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    // 如果状态中有值, 表示这次不是第一次, 判断上次和这次是否都大于10
                    Integer lastVc = lastVcState.value(); // 上一次的水位
                    if (lastVc != null) {
                        if (lastVc > 10 && value.getVc() > 10) {
                            out.collect(ctx.getCurrentKey() + "连续两次水位超过10, 发出红色预警");
                        }
                    }
                    // 第一次，将这一次的水位存入到状态
                    lastVcState.update(value.getVc());

                }
            })

            /*.flatMap(new RichFlatMapFunction<WaterSensor, String>() {

                private ValueState<Integer> lastVcState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Integer.class));
                }

                @Override
                public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                    Integer lastVc = lastVcState.value();
                    if (lastVc != null) {
                        if (lastVc > 10 && value.getVc() > 10) {
                            out.collect(value.getId() + "连续两次水位超过10, 发出红色预警");
                        }
                    }
                    lastVcState.update(value.getVc());
                }
            })*/
            .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
