package com.qf.rocketmq.base;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class BaseConsumer {

    public static void main(String[] args) throws MQClientException {
        //1.创建消费对象
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("my-consumer-group-1");
        //2.指明namesever的地址
        consumer.setNamesrvAddr("192.168.159.130:9876;192.168.159.131:9876;192.168.159.129:9876");
        //3.订阅主题：topic 和过滤消息用的tag表达式
        consumer.subscribe("MyTopic1", "*");
        //4.创建一个监听器，当broker把消息推过来调用
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt msg : list) {
                    System.out.println("收到的消息：" + new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //5.启动消费者
        consumer.start();
    }
}
