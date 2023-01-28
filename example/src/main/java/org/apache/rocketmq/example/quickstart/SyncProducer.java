package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.TimeUnit;

/**
 *
 * 同步发送
 */
public class SyncProducer {
    public static void main(String[] args) throws Exception{
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_20221124-222");
        // 设置NameServer的地址
        producer.setNamesrvAddr("127.0.0.1:9876");//106.55.246.66
        producer.setSendLatencyFaultEnable(true);
        // 设置发送超时时限为30s，默认3s  (本地测试，设置为3s,5s都不行)
        producer.setSendMsgTimeout(30000);
        // 启动Producer实例
        producer.start();
        //  等待几秒，防止生产者启动未完成就发送消息
        TimeUnit.SECONDS.sleep(4);
        for (int i = 0; i < 10; i++) {
            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message("TopicTestB" /* Topic */,
                    "TagA" /* Tag */,
                    ("测试Hello RocketMQ MQS" + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            // 发送消息到一个Broker
            SendResult sendResult = producer.send(msg);
            // 通过sendResult返回消息是否成功送达
            System.out.printf("%s%n", sendResult);
        }
        // 如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }
}
