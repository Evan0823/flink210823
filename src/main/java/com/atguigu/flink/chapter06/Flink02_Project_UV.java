package com.atguigu.flink.chapter06;

import com.atguigu.flink.bean.UserBehavior;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**网站独立访客数UV: Unique Visitor
 *      PV去重
 *
 * @author Evan
 * @ClassName Flink01_Project_PV
 * @date 2022-01-14 8:57
 */
public class Flink02_Project_UV {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env
            .readTextFile("input/UserBehavior.csv")
            .flatMap(new FlatMapFunction<String, UserBehavior>() {
                @Override
                public void flatMap(String line,
                                    Collector<UserBehavior> out) throws Exception {
                    String[] data = line.split(",");
                    UserBehavior ub = new UserBehavior(
                            Long.valueOf(data[0]),
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2]),
                            data[3],
                            Long.valueOf(data[4]));
                    if ("pv".equals(ub.getBehavior())) {
                        out.collect(ub);
                    }
                }
            })
                .keyBy(UserBehavior::getBehavior)
                .process(new KeyedProcessFunction<String, UserBehavior, Long>() {

                    HashSet<Long> uids = new HashSet<>();

                    @Override
                    public void processElement(UserBehavior value,
                                               Context ctx,
                                               Collector<Long> out) throws Exception {
                            /*int preUv = uids.size();
                        uids.add(value.getUserId());
                        int postUv = uids.size();
                        if (postUv > preUv) {

                            out.collect((long) postUv);
                        }*/

                        if(uids.add(value.getUserId())) {
                            out.collect((long) uids.size());
                        }
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
