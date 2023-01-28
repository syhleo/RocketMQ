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
package org.apache.rocketmq.client.impl.consumer;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.utils.ThreadUtils;

public class PullMessageService extends ServiceThread {
    private final InternalLogger log = ClientLogger.getLog();
    private final LinkedBlockingQueue<PullRequest> pullRequestQueue = new LinkedBlockingQueue<PullRequest>();
    private final MQClientInstance mQClientFactory;
    private final ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "PullMessageServiceScheduledThread");
                }
            });

    public PullMessageService(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    // 封装PullRequest放入阻塞队列 拉取消息 pullRequestQueue.put(pullRequest)
                    PullMessageService.this.executePullRequestImmediately(pullRequest);
                }
            }, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    public void executePullRequestImmediately(final PullRequest pullRequest) {
        try {
            this.pullRequestQueue.put(pullRequest);
        } catch (InterruptedException e) {
            log.error("executePullRequestImmediately pullRequestQueue.put", e);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(r, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    private void pullMessage(final PullRequest pullRequest) {
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(pullRequest.getConsumerGroup());
        if (consumer != null) {
            DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
            impl.pullMessage(pullRequest);
        } else {
            log.warn("No matched consumer for the PullRequest {}, drop it", pullRequest);
        }
    }

    /**
     * 客户端实例 MQClientInstance 中有一个单独的线程 PullMessageService 来定时拉取消息。
     * <p>
     * 消息拉取总结：启动消费者，消费者按照负载均衡策略生成拉取消息请求，设置拉取模式，读取消费进度，
     * 设置拉取起始偏移量，然后定时向Broker拉取消息。Broker接受拉取请求，
     * 从CommitLog查找消息，按照过滤策略处理消息后返回给消费者。若拉取任务失败，消息消费者修正偏移量再次拉取；若拉取成功，消费者进行消费，并更新消费进度，再次拉取。
     * ————————————————
     * 原文链接：https://blog.csdn.net/tang_2214/article/details/99302765
     * 
     */
    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                /**
                 * 从 LinkedBlockingQueue 中获取一个拉取请求，没有就阻塞
                 * 拉取请求（PullRequest）在客户端做负载均衡时会创建，一个拉取任务完成后还会继续拉取。
                 * 即从pullRequestQueue中获取拉取任务，如果获取不到，线程阻塞，直到有任务可被拉取
                 */
                PullRequest pullRequest = this.pullRequestQueue.take();
                this.pullMessage(pullRequest);
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                log.error("Pull Message Service Run Method exception", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public void shutdown(boolean interrupt) {
        super.shutdown(interrupt);
        ThreadUtils.shutdownGracefully(this.scheduledExecutorService, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getServiceName() {
        // 即 返回"PullMessageService"
        return PullMessageService.class.getSimpleName();
    }

}
