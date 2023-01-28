package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * @author: 里奥
 * @date: 2022/5/31 17:23
 * @description:
 */
public class Consumer {

    public static void main(String[] args) throws InterruptedException, MQClientException, MQClientException {

        // 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_12010101_mac");

        // 设置NameServer的地址
        consumer.setNamesrvAddr("localhost:9876");

        // 订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
        consumer.subscribe("TopicTestMQS1111", "*");
        // https://help.aliyun.com/document_detail/43523.html?spm=5176.rocketmq.help.dexternal.72d17d104HnRka
        // 再订阅另一个topic
        consumer.subscribe("TopicTestB1111", "Tag1||Tag3");
        //consumer.setConsumeMessageBatchMaxSize(1025);//[1, 1024]
        //广播模式消费
        //consumer.setMessageModel(MessageModel.BROADCASTING);


//        consumer.setMessageListener(new MessageListenerConcurrently() {
//            @Override
//            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
//                //System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
//                for (MessageExt msg : msgs) {
//                    System.out.println("消费的topic:" + msg.getTopic() + "  【消息】" + new String(msg.getBody()));
//                }
//                // 标记该消息已经被成功消费
//                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//            }
//        });

        // 注册回调实现类来处理从broker拉取回来的消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                //System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                for (MessageExt msg : msgs) {
                    System.out.println("消费的topic:" + msg.getTopic() + "  【消息】" + new String(msg.getBody()));
                    System.out.println("消息重试的次数：" + msg.getReconsumeTimes());
                    //这里设置重试大于3次 那么通过保存数据库 人工来兜底
                    if (msg.getReconsumeTimes() >= 2) {
                        // log.info("该消息已经重试3次,保存数据库。topic={},keys={},msg={}", msg.getTopic(), msg.getKeys(), body);
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                }
                //context.setAckIndex();
                //context.setDelayLevelWhenNextConsume();
                // 标记该消息已经被成功消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });


        // 启动消费者实例
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}