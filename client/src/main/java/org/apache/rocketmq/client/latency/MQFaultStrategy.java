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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;
    //todo 阿里的经验值
    //发送延时
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    //故障规避
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        //todo 默认不走这里：Broker故障延迟机制
        if (this.sendLatencyFaultEnable) {
            try {
                //todo 遍历队列，判断该Broker是否可用(),删除一段时间内不可用的Broker
                // 这里可以看到，Producer选择MessageQueue的方法，就是index自增，然后取模。并且只有这有一种方法
                int index = tpInfo.getSendWhichQueue().getAndIncrement(); //基于线程上下文的计数递增，用于轮询目的
                //对消息队列轮询获取一个队列
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    //基于index和队列数量取余，确定位置
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        return mq;
                }
                //todo 如果预测的所有broker都不可用，则随机选择一个broker,随机选择该Broker下一个队列进行发送 [因为要发送总得取一个messagequeue]
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                //获得Broker的写队列集合
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        //获得一个队列,指定broker和队列ID并返回
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }
            //  兜底选择 轮询
            return tpInfo.selectOneMessageQueue();
        }
        //todo 默认走这里
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            //获取不可用持续时长，在这个时间内，Broker将被规避（默认就是本次发送的延迟）
            //这里计算的
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }
    //todo 这里是一个根据发送延时来定义故障规避的时间
    //发送延时{50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    //故障规避{0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};
    //假如：消息发送时长为100毫秒，则mq预计broker的不可用时长为0毫秒
    //假如：消息发送时长为600毫秒，则mq预计broker的不可用时长为30000毫秒
    //假如：消息发送时长为4000毫秒，则mq预计broker的不可用时长为18000毫秒
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
