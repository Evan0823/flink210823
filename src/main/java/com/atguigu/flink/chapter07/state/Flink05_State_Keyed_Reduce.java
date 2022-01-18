package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**键控状态---聚合状态
 *
 * 需求：计算每个传感器的水位和
 *
 * @author Evan
 * @ClassName Flink03_State_Keyed_Value
 * @date 2022-01-18 11:15
 */
public class Flink05_State_Keyed_Reduce {
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

                    private ReducingState<Integer> sumVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sumVcState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>(
                                "sumVcState",
                                ((value1, value2) -> value1 + value2),
                                Integer.class
                        ));
                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        sumVcState.add(value.getVc());

                        out.collect(ctx.getCurrentKey() + "的水位和是:" + sumVcState.get());

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
