package com.qf.rocketmq.simple;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

//同步消息
public class SyncProducer {

    public static void main(String[] args) throws Exception {
        //1.创建生产者
        DefaultMQProducer producer = new DefaultMQProducer("producerGroup1");

        //2.生产者指定nameserver的地址
        producer.setNamesrvAddr("192.168.159.130:9876;192.168.159.131:9876;192.168.159.129:9876");
        //3.启动生产者
        producer.start();
        //4.创建消息
        for (int i = 0; i < 100; i++) {
            Message message = new Message("TopicTest", "TagA", ("hello RocketMQ:" + i).getBytes(StandardCharsets.UTF_8));
            //5。发送消息,默认3s，同步发送
            SendResult sendResult = producer.send(message);
            System.out.printf("%s%n", sendResult);
        }
        //6.关闭生产者
        producer.shutdown();
    }
}
