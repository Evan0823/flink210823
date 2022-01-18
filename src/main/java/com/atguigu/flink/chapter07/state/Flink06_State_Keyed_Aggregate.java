package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**键控状态---聚合状态
 *
 * 需求：计算每个传感器的平均水位
 *
 * @author Evan
 * @ClassName Flink03_State_Keyed_Value
 * @date 2022-01-18 11:15
 */
public class Flink06_State_Keyed_Aggregate {
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

                    private AggregatingState<WaterSensor, Double> avgVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        avgVcState = getRuntimeContext().getAggregatingState(
                                new AggregatingStateDescriptor<WaterSensor, Avg, Double>(
                                        "avgVcState",
                                        new AggregateFunction<WaterSensor, Avg, Double>() {
                                            @Override
                                            public Avg createAccumulator() {
                                                return new Avg();
                                            }

                                            @Override
                                            public Avg add(WaterSensor value, Avg acc) {
                                                acc.sum += value.getVc();
                                                acc.count++;
                                                return acc;
                                            }

                                            @Override
                                            public Double getResult(Avg acc) {
                                                return acc.sum * 1.0 / acc.count;
                                            }

                                            @Override
                                            public Avg merge(Avg a, Avg b) {
                                                return null;
                                            }
                                        },
                                        Avg.class
                                )
                        );
                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {

                        avgVcState.add(value);

                        out.collect(ctx.getCurrentKey() + " 的平均水位是: " + avgVcState.get());
                    }
                })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class Avg {
        public Integer sum = 0;
        public Long count = 0L;
    }

}
