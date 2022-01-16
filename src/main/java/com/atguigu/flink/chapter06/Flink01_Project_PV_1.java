package com.atguigu.flink.chapter06;

import com.atguigu.flink.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Evan
 * @ClassName Flink01_Project_PV
 * @date 2022-01-14 8:57
 */
public class Flink01_Project_PV_1 {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);


        try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
    }
}
