package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author: 里奥
 * @date: 2022/5/31 17:23
 * @description:
 */
public class MultiConsumer3 {

    public static void main(String[] args) throws InterruptedException, MQClientException, MQClientException {

        // 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_1201_mac");

        // 此处干脆不设置NameServer的地址 ，让HTTP静态服务器寻址方式，好处是客户端部署简单，且Name Server集群可以热升级。
         consumer.setNamesrvAddr("localhost:9876");

        // 订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
        consumer.subscribe("TopicTestMQS", "*");
        // https://help.aliyun.com/document_detail/43523.html?spm=5176.rocketmq.help.dexternal.72d17d104HnRka
        //consumer.setConsumeMessageBatchMaxSize(1025);//[1, 1024]
        //用户同机房分配策略
//        consumer.setAllocateMessageQueueStrategy(new AllocateMachineRoomNearby(new AllocateMessageQueueAveragely()
//                ,new MyMachineResolver()));

        // 注册回调实现类来处理从broker拉取回来的消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("[consumer]%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs.size()); // ConsumeMessageThread_3 Receive New Messages: 1
                //默认情况下，每次都只是一条数据
                for (MessageExt msg : msgs) {
                    System.out.println("[consumer]"+Thread.currentThread().getName()+"消费的topic:" + msg.getTopic() + "  【消息】" + new String(msg.getBody()));
                }
                // 标记该消息已经被成功消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });



        // 实例化消费者
        DefaultMQPushConsumer consumer2 = new DefaultMQPushConsumer("test_1822_mac");

        // 此处干脆不设置NameServer的地址 ，让HTTP静态服务器寻址方式，好处是客户端部署简单，且Name Server集群可以热升级。
        consumer2.setNamesrvAddr("localhost:9876");

        // 订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
        // https://help.aliyun.com/document_detail/43523.html?spm=5176.rocketmq.help.dexternal.72d17d104HnRka
        consumer2.subscribe("TopicTestB", "*");
        //consumer.setConsumeMessageBatchMaxSize(1025);//[1, 1024]
        //用户同机房分配策略
//        consumer2.setAllocateMessageQueueStrategy(new AllocateMachineRoomNearby(new AllocateMessageQueueAveragely()
//                ,new MyMachineResolver()));

        // 注册回调实现类来处理从broker拉取回来的消息
        consumer2.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("[consumer2] %s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs.size()); // ConsumeMessageThread_3 Receive New Messages: 1
                //默认情况下，每次都只是一条数据
                for (MessageExt msg : msgs) {
                    System.out.println("[consumer2]"+Thread.currentThread().getName()+"消费的topic:" + msg.getTopic() + "  【消息】" + new String(msg.getBody()));
                }
                // 标记该消息已经被成功消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });


        // 启动消费者实例
        consumer2.start();
        System.out.printf("Consumer2 Started.%n");

        // 启动消费者实例
        consumer.start();
        System.out.printf("Consumer Started.%n");

    }
}