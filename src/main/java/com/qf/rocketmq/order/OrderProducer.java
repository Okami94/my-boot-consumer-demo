package com.qf.rocketmq.order;


import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.nio.charset.StandardCharsets;
import java.util.List;

//顺序生产者
public class OrderProducer {

    public static void main(String[] args) throws Exception {

        MQProducer producer = new DefaultMQProducer("example_group_name");
        //需要在环境变量中配置NAMESRV_ADDR=192.168.159.130:9876
        producer.start();

        for (int i = 0; i < 10; i++) {
            int orderId = i;
            for (int j = 0; j <= 5; j++) {
                Message msg = new Message("OrderTopicTest", "order_" + orderId, "KEY" + orderId, ("order_" + orderId + " step " + j).getBytes(RemotingHelper.DEFAULT_CHARSET));
                //选中某个队列
                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                        Integer id = (Integer) o;
                        int index = id % list.size();
                        return list.get(index);
                    }
                }, orderId);
                System.out.printf("%s%n", sendResult);
            }

        }

    }
}

