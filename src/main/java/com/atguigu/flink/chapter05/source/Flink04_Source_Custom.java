package com.atguigu.flink.chapter05.source;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;


/**Source--->自定义Source
 *
 * @author Evan
 * @ClassName Flink01_Source_File
 * @date 2022-01-12 14:47
 */
public class Flink04_Source_Custom {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new MySource("hadoop102", 9999)).print();

        env.execute();

    }

    public  static class MySource implements SourceFunction<WaterSensor>{

        private String host;
        private int port;

        private boolean stop = false;

        public MySource(String host, int port){
            this.host = host;
            this.port = port;
        }

        // source的核心方法, 从socket读取数据, 并把读到的数据放入到流中
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {

            Socket socket = new Socket(host,port);
            InputStream is = socket.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            String line = reader.readLine();
            while (!stop && line != null) {
                String[] data = line.split(",");
                ctx.collect(new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2])));

                line = reader.readLine();
            }
        }

        // 当外面调用这个方法的时候,用于停止source
        @Override
        public void cancel() {
                stop = true;
        }
    }
}
