package com.atguigu.flink.chapter06;

import com.atguigu.flink.bean.OrderEvent;
import com.atguigu.flink.bean.TxEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**订单支付实时监控
 *      来自两条流(订单支付与到账)的订单交易匹配
 *
 * @author Evan
 * @ClassName Flink04_Order
 * @date 2022-01-14 10:15
 */
public class Flink04_Project_Order {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        SingleOutputStreamOperator<OrderEvent> orderStream = env
                .readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(data[0]),
                            data[1],
                            data[2],
                            Long.valueOf(data[3])
                    );
                })
                .filter(log -> "pay".equals(log.getEventType()));

        SingleOutputStreamOperator<TxEvent> txStream = env
                .readTextFile("input/ReceiptLog.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new TxEvent(
                            data[0],
                            data[1],
                            Long.valueOf(data[2])
                    );
                });

        orderStream
                .connect(txStream)
                .keyBy(OrderEvent::getTxId, TxEvent::getTxId)
                .process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
                    // key交易id  value:
                    HashMap<String, OrderEvent> orderMap = new HashMap<>();
                    Map<String, TxEvent> txMap =   new HashMap<String, TxEvent>();
                    @Override
                    public void processElement1(OrderEvent value,
                                                Context ctx,
                                                Collector<String> out) throws Exception {
                        // 如果支付信息来了, 则判断交易信息是否存在, 如果存在就对账成功, 否则就把这次的支付信息保存起来
                        String txId = value.getTxId();
                        if(txMap.containsKey(txId)) {
                            out.collect("订单：" + value.getOrderId() + "对账成功");
                        }else{
                            orderMap.put(txId, value);
                        }
                    }

                    @Override
                    public void processElement2(TxEvent value,
                                                Context ctx,
                                                Collector<String> out) throws Exception {
                        String txId = value.getTxId();
                        if (orderMap.containsKey(txId)) {
                            out.collect("订单: " + orderMap.get(txId).getOrderId() + "对账成功");
                        }else{
                            txMap.put(txId, value);
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
