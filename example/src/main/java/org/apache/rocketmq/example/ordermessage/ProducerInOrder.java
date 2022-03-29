/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.ordermessage;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
/**
 * @author 【享学课堂】 King老师
 * 部分顺序消息生产
 */
public class ProducerInOrder {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("OrderProducer");
        producer.setNamesrvAddr("106.55.246.66:9876");//106.55.246.66
        producer.start();

        String[] tags = new String[]{"TagA", "TagC", "TagD"};
        // 订单列表
        List<Order> orderList = new ProducerInOrder().buildOrders();

        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateStr = sdf.format(date);
        for (int i = 0; i < orderList.size(); i++) {
            // 加个时间前缀
            String body = dateStr + " Order:" + orderList.get(i);
            Message msg = new Message("PartOrder", tags[i % tags.length], "KEY" + i, body.getBytes());

            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Long id = (Long) arg;  //根据订单id选择发送queue
                    long index = id % mqs.size();
                    return mqs.get((int) index);
                }
            }, orderList.get(i).getOrderId());//订单id

            System.out.println(String.format("SendResult status:%s, queueId:%d, body:%s",
                    sendResult.getSendStatus(),
                    sendResult.getMessageQueue().getQueueId(),
                    body));
        }

        producer.shutdown();
    }

    /**
     * 订单
     */
    private static class Order {
        private long orderId;
        private String desc;

        public long getOrderId() {
            return orderId;
        }

        public void setOrderId(long orderId) {
            this.orderId = orderId;
        }

        public String getDesc() {
            return desc;
        }

        public void setDesc(String desc) {
            this.desc = desc;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "orderId=" + orderId +
                    ", desc='" + desc + '\'' +
                    '}';
        }
    }

    /**
     * 生成模拟订单数据  3个订单   每个订单4个状态
     */
    private List<Order> buildOrders() {
        List<Order> orderList = new ArrayList<Order>();

        Order orderDemo = new Order();
        orderDemo.setOrderId(20210406001L);
        orderDemo.setDesc("创建");
        orderList.add(orderDemo);

        orderDemo = new Order();
        orderDemo.setOrderId(20210406002L);
        orderDemo.setDesc("创建");
        orderList.add(orderDemo);

        orderDemo = new Order();
        orderDemo.setOrderId(20210406001L);
        orderDemo.setDesc("付款");
        orderList.add(orderDemo);

        orderDemo = new Order();
        orderDemo.setOrderId(20210406003L);
        orderDemo.setDesc("创建");
        orderList.add(orderDemo);

        orderDemo = new Order();
        orderDemo.setOrderId(20210406002L);
        orderDemo.setDesc("付款");
        orderList.add(orderDemo);

        orderDemo = new Order();
        orderDemo.setOrderId(20210406003L);
        orderDemo.setDesc("付款");
        orderList.add(orderDemo);

        orderDemo = new Order();
        orderDemo.setOrderId(20210406002L);
        orderDemo.setDesc("推送");
        orderList.add(orderDemo);

        orderDemo = new Order();
        orderDemo.setOrderId(20210406003L);
        orderDemo.setDesc("推送");
        orderList.add(orderDemo);

        orderDemo = new Order();
        orderDemo.setOrderId(20210406002L);
        orderDemo.setDesc("完成");
        orderList.add(orderDemo);

        orderDemo = new Order();
        orderDemo.setOrderId(20210406001L);
        orderDemo.setDesc("推送");
        orderList.add(orderDemo);

        orderDemo = new Order();
        orderDemo.setOrderId(20210406003L);
        orderDemo.setDesc("完成");
        orderList.add(orderDemo);

        orderDemo = new Order();
        orderDemo.setOrderId(20210406001L);
        orderDemo.setDesc("完成");
        orderList.add(orderDemo);

        return orderList;
    }
}
