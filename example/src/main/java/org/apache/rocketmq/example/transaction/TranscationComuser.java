package org.apache.rocketmq.example.transaction;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * @author 【享学课堂】 King老师
 * 事务消息-消费者 B
 */
public class TranscationComuser {
    public static void main(String[] args) throws Exception {
        // 实例化消息生产者,指定组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("TranscationComsuer");
        // 指定Namesrv地址信息.
        consumer.setNamesrvAddr("106.55.246.66:9876");
        // 订阅Topic
        consumer.subscribe("TransactionTopic", "*");
        //负载均衡模式消费
        consumer.setMessageModel(MessageModel.CLUSTERING);
        // 注册回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                try {
                    //todo  1、开启事务
                    //todo @Transaction
                    for(MessageExt msg : msgs) {
                        //todo 执行本地事务 update B...（幂等性）
                        System.out.println("update B ... where transactionId:"+msg.getTransactionId());
                        //todo 本地事务成功
                        System.out.println("commit:"+msg.getTransactionId());
                        System.out.println("执行本地事务成功，确认消息");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("执行本地事务失败，重试消费，尽量确保B处理成功");
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动消息者
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
