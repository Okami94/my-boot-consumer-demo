package com.qf.rocketmq.transaction;


import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

public class TransactionListenerImpl implements TransactionListener {
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        String tags=message.getTags();
        if(StringUtils.contains(tags,"TagA")){
            return LocalTransactionState.COMMIT_MESSAGE;
        }else if(StringUtils.contains(tags,"TagB")){
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }else{
            return LocalTransactionState.UNKNOW;
        }

    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        String tags=messageExt.getTags();

        if(StringUtils.contains(tags,"TagC")){
            return LocalTransactionState.COMMIT_MESSAGE;
        }else if(StringUtils.contains(tags,"TagD")){
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }else{
            return LocalTransactionState.UNKNOW;
        }
    }
}
