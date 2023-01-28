package org.apache.rocketmq.example.scheduled;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
/**
 *
 * 延时消息-生产者
 */
public class ScheduledMessageProducer {
    public static void main(String[] args) throws Exception {
        // 实例化一个生产者来产生延时消息
        DefaultMQProducer producer = new DefaultMQProducer("ScheduledProducerA");
        // 设置NameServer的地址
        producer.setNamesrvAddr("localhost:9876");
        // 设置发送超时时限为30s，默认3s
        producer.setSendMsgTimeout(30000);
        // 启动Producer实例
        producer.start();
        int totalMessagesToSend = 1;
        for (int i = 0; i < totalMessagesToSend; i++) {
            Message message = new Message("TopicTestMQS", ("Hello scheduled message " + i).getBytes());
            // 设置延时等级3,这个消息将在10s之后投递给消费者(详看delayTimeLevel)
            // delayTimeLevel：(1~18个等级)"1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
            message.setDelayTimeLevel(1); // 5--->1m
            // 发送消息    这里就是发送延时消息
            producer.send(message);
        }
        // 关闭生产者
        producer.shutdown();
    }
}
/*
 首先，producer发送的延迟消息，会进入SCHEDULE_TOPIC_XXXX的系统TOPIC的队列中（比如：你设置message.setDelayTimeLevel(5)，对应延迟1m,SCHEDULE_TOPIC_XXXX的queueId=4）
 然后，到了1m后，ScheduledTopicA的messageQueue才有消息【broker会专门处理SCHEDULE_TOPIC_XXXX的消息】，从而Consumer能立马消费到ScheduledTopicA里面的消息。
 【此处由ScheduleMessageService专门处理延迟消息，当1m达到后，会把SCHEDULE_TOPIC_XXXX里面的queueId=4上面存的延迟消息重新保存到commitLog中，
 此时ScheduledTopicA的messageQueue就有消息了，它的maxOffset会增加的。不管订阅ScheduledTopicA的consumer是否启动，1m后ScheduledTopicA的messageQueue都会有消息】
 注意：producer发送延迟消息出去后，是先到SCHEDULE_TOPIC_XXXX队列中进行存储。
 SCHEDULE_TOPIC_XXXX不存在任何的Consumer订阅组（即Consumer是不能直接消费SCHEDULE_TOPIC_XXXX里面的消息的）
 SCHEDULE_TOPIC_XXXX存储所有的延迟消息，不区分topic

 定时消息会暂存在名为SCHEDULE_TOPIC_XXXX的topic中，并根据delayTimeLevel存入特定的queue，
 queueId =delayTimeLevel – 1，即一个queue只存相同延迟的消息，保证具有相同发送延迟的消息能够顺序消费。
 broker会调度地消费SCHEDULE_TOPIC_XXXX，将消息写入真实的topic。
 topic:SCHEDULE_TOPIC_XXXX
 queueId: delayLevel - 1（延迟级别-1）
 */