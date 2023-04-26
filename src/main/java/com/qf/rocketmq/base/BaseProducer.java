package com.qf.rocketmq.base;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

public class BaseProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        //1.创建生产者
        DefaultMQProducer producer = new DefaultMQProducer("my-producer-group1");

        //2.生产者指定nameserver的地址
        producer.setNamesrvAddr("192.168.159.130:9876;192.168.159.131:9876;192.168.159.129:9876");

        //producer.setNamesrvAddr("192.168.159.130:9876");
        //3.启动生产者
        producer.start();
        //4.创建消息
        for (int i = 0; i < 10; i++) {
            Message message = new Message("MyTopic1", "TagA", ("hello rocketmq:" + i).getBytes(StandardCharsets.UTF_8));
            //5。发送消息,默认3s，超时会报错
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }
        //6.关闭生产者
        producer.shutdown();

    }
}
