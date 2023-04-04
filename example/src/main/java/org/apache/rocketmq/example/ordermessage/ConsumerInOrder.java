package org.apache.rocketmq.example.ordermessage;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
/**
 *
 * 部分顺序消息消费
 */
public class ConsumerInOrder {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("OrderConsumer");
        consumer.setNamesrvAddr("localhost:9876");//106.55.246.66
        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("PartOrder", "*");
        consumer.registerMessageListener(new MessageListenerOrderly() {
            Random random = new Random();
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                context.setAutoCommit(true);
                for (MessageExt msg : msgs) {
                    // 可以看到每个queue有唯一的consume线程来消费, 订单对每个queue(分区)有序
                    System.out.println("consumeThread=" + Thread.currentThread().getName() + "queueId=" + msg.getQueueId() + ", content:" + new String(msg.getBody()));
                }
                try {
                    //模拟业务逻辑处理中...
                    TimeUnit.MILLISECONDS.sleep(random.nextInt(300));
                } catch (Exception e) {
                    e.printStackTrace();
                    //这个点要注意：意思是先等一会，一会儿再处理这批消息，而不是放到重试队列里
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
        System.out.println("Consumer Started.");
    }
}

/*
Consumer Started.
consumeThread=ConsumeMessageThread_2queueId=3, content:2023-01-12 15:00:12 Order:Order{orderId=20210406003, desc='创建'}
consumeThread=ConsumeMessageThread_3queueId=1, content:2023-01-12 15:00:12 Order:Order{orderId=20210406001, desc='创建'}
consumeThread=ConsumeMessageThread_1queueId=2, content:2023-01-12 15:00:12 Order:Order{orderId=20210406002, desc='创建'}
consumeThread=ConsumeMessageThread_2queueId=3, content:2023-01-12 15:00:12 Order:Order{orderId=20210406003, desc='付款'}
consumeThread=ConsumeMessageThread_1queueId=2, content:2023-01-12 15:00:12 Order:Order{orderId=20210406002, desc='付款'}
consumeThread=ConsumeMessageThread_3queueId=1, content:2023-01-12 15:00:12 Order:Order{orderId=20210406001, desc='付款'}
consumeThread=ConsumeMessageThread_2queueId=3, content:2023-01-12 15:00:12 Order:Order{orderId=20210406003, desc='推送'}
consumeThread=ConsumeMessageThread_3queueId=1, content:2023-01-12 15:00:12 Order:Order{orderId=20210406001, desc='推送'}
consumeThread=ConsumeMessageThread_1queueId=2, content:2023-01-12 15:00:12 Order:Order{orderId=20210406002, desc='推送'}
consumeThread=ConsumeMessageThread_3queueId=1, content:2023-01-12 15:00:12 Order:Order{orderId=20210406001, desc='完成'}
consumeThread=ConsumeMessageThread_2queueId=3, content:2023-01-12 15:00:12 Order:Order{orderId=20210406003, desc='完成'}
consumeThread=ConsumeMessageThread_1queueId=2, content:2023-01-12 15:00:12 Order:Order{orderId=20210406002, desc='完成'}

 */