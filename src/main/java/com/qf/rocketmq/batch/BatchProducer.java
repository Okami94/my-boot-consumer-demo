package com.qf.rocketmq.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class BatchProducer {

    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
        producer.start();
        String topic="BatchTest";
        List<Message> messages=new ArrayList<>();
        messages.add(new Message(topic,"TagA","OrderID001","Hello world 0".getBytes()));
        messages.add(new Message(topic,"TagA","OrderID002","Hello world 1".getBytes()));
        messages.add(new Message(topic,"TagA","OrderID003","Hello world 2".getBytes()));
        producer.send(messages);
        producer.shutdown();


    }
}
