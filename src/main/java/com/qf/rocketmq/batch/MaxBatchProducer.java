package com.qf.rocketmq.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;


import java.util.ArrayList;
import java.util.List;


//批量消息实际大小不能超过4M。如果超过4m的批量消息需要进行分批处理，同时设定broker的配置broker的配置参数为4m
//在broker的配置文件中修改：maxMessageSize=4194304
public class MaxBatchProducer {

    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("BatchProducerGroupName");
        producer.start();
        String topic = "BatchTest";

        List<Message> messages = new ArrayList<>(100);

        for (int i = 0; i < 100 * 1000; i++) {
            messages.add(new Message(topic, "OrderID" + i, ("Hello Scheduled message" + i).getBytes()));
        }

        ListSplitter splitter = new ListSplitter(messages);

        while (splitter.hasNext()) {
            List<Message> listItem = splitter.next();
            producer.send(listItem);
        }
        producer.shutdown();
    }
}
