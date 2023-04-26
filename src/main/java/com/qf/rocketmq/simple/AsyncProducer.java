package com.qf.rocketmq.simple;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


//异步消息
public class AsyncProducer {


    public static void main(String[] args) throws Exception {
        //1.创建生产者
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group");

        //2.生产者指定nameserver的地址
        producer.setNamesrvAddr("192.168.159.130:9876;192.168.159.131:9876;192.168.159.129:9876");
        //3.启动生产者
        producer.start();

        //异步消息发送失败重试次数
        producer.setRetryTimesWhenSendAsyncFailed(0);

        int messageCount=100;
        final CountDownLatch countDownLatch=new CountDownLatch(messageCount);
        //4.创建消息
        for (int i = 0; i < messageCount; i++) {
            try {
                final int index = i;
                Message message = new Message("Jodie_topic_1023", "TagA", "OrderID188", ("hello RocketMQ:" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                //5。发送消息,默认3s，同步发送
                producer.send(message, new SendCallback() {
                    @Override
                    //回调成功
                    public void onSuccess(SendResult sendResult) {
                        //数量减一
                        countDownLatch.countDown();
                        System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                    }
                    @Override
                    //回调失败
                    public void onException(Throwable throwable) {
                        countDownLatch.countDown();
                        System.out.printf("%-10d Exception %s %n", index, throwable);
                        throwable.printStackTrace();
                    }
                });
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        System.out.println("=================");
        //等countDownLatch减到0才会往下走
        countDownLatch.await(5, TimeUnit.SECONDS);
        //6.关闭生产者
        producer.shutdown();
    }
}
