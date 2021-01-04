package com.scb.rocketmq.com.scb.rocketmq.producer;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;

/**
 * @author suchaobin
 * @description 事务消息生产者
 * @date 2021/1/4 16:06
 **/
public class TransactionProducer {
    public static void main(String[] args) throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("TransactionMQ");
        producer.setNamesrvAddr("localhost:9876");
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                if ("tagA".equalsIgnoreCase(message.getTags())) {
                    return LocalTransactionState.COMMIT_MESSAGE;
                }
                if ("tagB".equalsIgnoreCase(message.getTags())) {
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
                return LocalTransactionState.UNKNOW;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                String keys = messageExt.getKeys();
                if ("tagC".equals(keys)) {
                    return LocalTransactionState.COMMIT_MESSAGE;
                }
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }
        });
        producer.start();
        String[] tags = {"tagA", "tagB", "tagC"};
        for (int i = 0; i < 3; i++) {
            String content = "test" + i;
            Message message = new Message("transactionTopic", tags[i], content.getBytes(StandardCharsets.UTF_8));
            message.setKeys(tags[i]);
            TransactionSendResult sendResult = producer.sendMessageInTransaction(message, null);
            System.err.println(sendResult);
            Thread.sleep(1000);
        }
    }
}
