package com.qf.rocketmq.scheduled;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;


//延迟消息
public class ScheduledProducer {
    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("ExampleProducerGroup");
        producer.start();
        int totalMessageToSend = 100;
        for (int i = 0; i < totalMessageToSend; i++) {
            Message message = new Message("TestTopic", ("Hello Scheduled message" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            //延迟级别
            message.setDelayTimeLevel(3);

            producer.send(message);

        }
        producer.shutdown();

    }
}
