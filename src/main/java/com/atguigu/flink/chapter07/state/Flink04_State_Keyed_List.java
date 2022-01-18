package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;

/**键控状态---列表状态(存多个值)
 *
 *      需求：针对每个传感器输出最高的3个水位值
 *
 *      process中或map/flatMap富有版本中使用
 *
 *
 * @author Evan
 * @ClassName Flink03_State_Keyed_Value
 * @date 2022-01-18 11:15
 */
public class Flink04_State_Keyed_List {
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
                private ListState<Integer> top3State;

                @Override
                public void open(Configuration parameters) throws Exception {
                    top3State = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("top3State", Integer.class));
                }

                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    top3State.add(value.getVc());

                    // 把状态中所有的值取出, 排序(降序), 只保留前3个
                    Iterable<Integer> it = top3State.get();
                    /**
                     * 只有将非容器式的转成容器式的才能对数据进行排序
                     */
                    List<Integer> list = AtguiguUtil.toList(it);
                     /*list.sort(new Comparator<Integer>() {
                        @Override
                        public int compare(Integer o1, Integer o2) {
                            return o2.compareTo(o1);
                        }
                    });*/
                    //list.sort((o1, o2) -> o2.compareTo(o1)); // 简化成lambda，还可以进一步简化
                    list.sort(Comparator.reverseOrder()); // 原地排序: list本身会变的有效

                    if (list.size() > 3) {
                        list.remove(list.size() - 1); // 删除最小的元素
                    }

                    top3State.update(list); //用list中的元素覆盖状态中所有的值

                    out.collect(list.toString());

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