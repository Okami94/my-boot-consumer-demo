package com.qf.rocketmq.batch;


import org.apache.rocketmq.common.message.Message;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;

public class ListSplitter implements Iterator<List<Message>> {

    private int sizeLimit = 1000 * 1000;
    private final List<Message> messages;
    private int currIndex;

    public ListSplitter(List<Message> messages) {

        this.messages = messages;
    }


    @Override
    public boolean hasNext() {

        return false;
    }

    @Override
    public List<Message> next() {
        int nextIndex = currIndex;
        int totalSize = 0;
        for (; nextIndex < messages.size(); nextIndex++) {
            Message message = messages.get(nextIndex);
            int tmpSize = message.getTopic().length() + message.getBody().length;
            Map<String, String> properties = message.getProperties();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                tmpSize += entry.getKey().length() + entry.getValue().length();
            }
            tmpSize = tmpSize + 20;
            if (tmpSize > sizeLimit) {
                if (nextIndex - currIndex == 0) {
                    nextIndex++;
                }
                break;
            }
            if (tmpSize + totalSize > sizeLimit) {
                break;

            }else{
                totalSize+=tmpSize;
            }
        }
        List<Message> subList=messages.subList(currIndex,nextIndex);
        currIndex=nextIndex;
        return subList;
    }

    @Override
    public void remove() {

    }

    @Override
    public void forEachRemaining(Consumer<? super List<Message>> action) {

    }
}
