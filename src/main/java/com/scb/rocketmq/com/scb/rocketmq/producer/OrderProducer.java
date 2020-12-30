package com.scb.rocketmq.com.scb.rocketmq.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

/**
 * @author suchaobin
 * @description 顺序消息生产者
 * @date 2020/12/30 14:38
 **/
public class OrderProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("testGroup4");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        for (int i = 0; i < 10; i++) {
            byte[] bytes = ("hello world" + i).getBytes(RemotingHelper.DEFAULT_CHARSET);
            int orderId = i + 10;
            producer.send(new Message("orderTopic", "tag", i + "", bytes),
                    new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    int index = orderId % list.size();
                    return list.get(index);
                }
            }, orderId);
        }
        producer.shutdown();
    }
}
