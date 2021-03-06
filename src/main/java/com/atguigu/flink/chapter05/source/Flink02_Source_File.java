package com.atguigu.flink.chapter05.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**Source--->从文件读取数据(hdfs上)
 *
 * @author Evan
 * @ClassName Flink01_Source_File
 * @date 2022-01-12 14:47
 */
public class Flink02_Source_File {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<String> stream = env.readTextFile("hdfs://hadoop102:8020/words.txt");
        stream.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
