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
package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueueExt;

public class PullRequestHoldService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final String TOPIC_QUEUEID_SEPARATOR = "@";
    private final BrokerController brokerController;
    private final SystemClock systemClock = new SystemClock();
    /**
     * pullRequestTable保存的都是挂起的请求
     */
    private ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable =
        new ConcurrentHashMap<String, ManyPullRequest>(1024);

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        mpr.addPullRequest(pullRequest);
    }

    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                // gdtodo: Consumer订阅消息时，Broker是否开启长轮询
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    // broker端开启长轮询，等待5s再尝试拉取
                    this.waitForRunning(5 * 1000);
                } else {
                    // 没有开启长轮询，等待1秒再次尝试拉取
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();
                // 利用checkHoldRequest方法来判断是否有新消息的产生（看看有新消息到了没）
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

    private void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try {
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                }
            }
        }
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null, 0, null, null);
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode,
        long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        String key = this.buildKey(topic, queueId);
        // 拿到topic+queueId对应的PullRequest
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            // 获取主题与队列的所有 PullRequest 并清除内部 pullRequest 集合，避免重复拉取。
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                List<PullRequest> replayList = new ArrayList<PullRequest>();

                for (PullRequest request : requestList) {
                    long newestOffset = maxOffset;
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    }
                    // 当前最新的offset大于请求的offset，也就是有消息到来
                    if (newestOffset > request.getPullFromThisOffset()) {
                        // 进行消息过滤
                        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                            new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                        // match by bit map, need eval again when properties is not null.
                        if (match && properties != null) {
                            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                        }
                        /**
                         * 如果消息匹配后，则调用executeRequestWhenWakeup将消息返回给客户端，否则等待下一次尝试。
                         * executeRequestWhenWakeup里面也是会再调用一次拉取消息的过程 消息查询的流程
                         * 即调用executeRequestWhenWakeup 进行消息拉取，并返回结果到客户端。
                         */
                        if (match) {
                            try {
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error("execute request when wakeup failed.", e);
                            }
                            continue;
                        }
                    }
                    /**
                     * 如果挂起超时时间超时，则不继续等待将直接返回客户端消息未找到。
                     * 如果这次还没有消息，也会直接返回哈，避免hold的时间太长
                     * request.getTimeoutMillis():BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15; 15s
                     * request.getSuspendTimestamp():this.brokerController.getMessageStore().now() 长轮询开始时间
                     * PullRequest pullRequest = new PullRequest(request, channel, pollingTimeMills,
                     *                                 this.brokerController.getMessageStore().now(), offset, subscriptionData, messageFilter);
                     * 所以长轮询中Broker默认最长阻塞时间为15秒(consumer请求等待的最长时间是30秒）
                     */
                    // 当前时间 >= 请求hold时间 + 请求超时时间
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            // 长轮询超时了之后会再调用一次拉取消息的方法（PullMessageProcessor#processRequest），
                            // 但是这一次调用就不会触发长轮询了，所以消费者就会收到不等于null的response响应，然后主动地再去向broker发起下一次拉消息请求
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }
                        continue;
                    }

                    replayList.add(request);
                }
                // 未到超时&也没有新消息的PullRequest，再次放入ManyPullRequest
                if (!replayList.isEmpty()) {
                    // 把PullRequest放回ManyPullRequest中，下一次循环的时候会继续拿到这个PullRequest
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }
}
