package com.scb.rocketmq.com.scb.rocketmq.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author suchaobin
 * @description 单向消息生产者
 * @date 2020/12/30 11:47
 **/
public class OnewayProducer {
    public static void main(String[] args) throws Exception {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("testGroup3");
        // 设置NameServer的地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动Producer实例
        producer.start();
        for (int i = 0; i < 100; i++) {
            // 创建消息，并指定Topic，Tag和消息体
            byte[] bytes = ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET);
            Message msg = new Message("TopicTest3", "TagA", bytes);
            // 发送消息到一个Broker
            producer.sendOneway(msg);
        }
        // 如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }
}
