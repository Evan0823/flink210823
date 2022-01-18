package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**键控状态---Map状态
 *
 *     去重：去掉重复的水位值. 思路: 把水位值作为MapState的key来实现去重, value随意
 *
 *
 * @author Evan
 * @ClassName Flink03_State_Keyed_Value
 * @date 2022-01-18 11:15
 */
public class Flink07_State_Keyed_Map {
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

                private MapState<Integer, Object> vcMapState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    vcMapState = getRuntimeContext().getMapState(
                            new MapStateDescriptor<Integer, Object>("vcMapState", Integer.class, Object.class));
                }

                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    vcMapState.put(value.getVc(), new Object());

                    Iterable<Integer> keys = vcMapState.keys();
                    List<Integer> vcs = AtguiguUtil.toList(keys);
                    out.collect(ctx.getCurrentKey() + "的不重复的水位值:" + vcs);
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
