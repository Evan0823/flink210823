package com.atguigu.flink.chapter06;

import com.atguigu.flink.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**市场营销商业指标---APP的下载安装更新卸载
 *
 *
 * @author Evan
 * @ClassName Flink03_Project_App
 * @date 2022-01-14 9:56
 */
public class Flink03_Project_App {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env
                .addSource(new AppSource())
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                        return Tuple2.of(value.getBehavior(), 1L);
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1)
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class AppSource implements SourceFunction<MarketingUserBehavior> {

        Random random = new Random();

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            String[] behaviors = {"download", "install", "update", "uninstall"};
            String[] channels = {"小米市场", "华为市场", "appStore", "oppo", "vivo"};

            while(true) {
                Long userId  = (long)random.nextInt(2000);
                String behavior = behaviors[random.nextInt(behaviors.length)];
                String channel = channels[random.nextInt(channels.length)];
                long timestamp = System.currentTimeMillis();

                ctx.collect(new MarketingUserBehavior(userId, behavior, channel, timestamp));
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {

        }
    }
}
