package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import com.mysql.fabric.FabricStateResponse;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.List;

/**给程序设置状态后端
 *
 *
 * @author Evan
 * @ClassName Flink08_State_Backend
 * @date 2022-01-18 15:15
 */
public class Flink08_State_Backend {
    public static void main(String[] args) throws IOException {

        System.setProperty("HADOOP_USER_NAME","atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.enableCheckpointing(1000);// checkpoint周期
        /**
         * 1.内存
         *      1.1旧的写法
         *      1.2新的写法
         * 2.fs
         *      2.1旧的
         *      2.2新的
         *
         * 3.rockdb
         *      3.1旧的
         *      3.2新的
         */
        // 1.1旧的写法
        //env.setStateBackend(new MemoryStateBackend()); //默认的
        // 1.2新的写法
//        env.setStateBackend(new HashMapStateBackend()); // 本地使用内存
//        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage()); // 设置checkpoint的内存

//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/ck"));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck1");

//        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/ck2"));
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck3");

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
