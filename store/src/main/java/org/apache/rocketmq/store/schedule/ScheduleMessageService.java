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
package org.apache.rocketmq.store.schedule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;

    /**
     * 延迟级别缓存
     * 这里会保存level对应的delay timeMillis
     * key=>延迟级别,一个延迟级别对应一个延迟队列
     * value=>对应的延迟时间
     */
    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
        new ConcurrentHashMap<Integer, Long>(32);

    /**
     * key=>延迟级别,一个延迟级别对应一个延迟队列
     * value=>延迟队列的消费进度 即offsetTable存放延延迟级别对应的队列消费的offset
     * broker启动的时候这个map的数据会从delayOffset.json文件中获取
     *
     */
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
        new ConcurrentHashMap<Integer, Long>(32);
    private final DefaultMessageStore defaultMessageStore;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private Timer timer;
    private MessageStore writeMessageStore;
    private int maxDelayLevel;

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.writeMessageStore = defaultMessageStore;
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    /**
     * @param writeMessageStore
     *     the writeMessageStore to set
     */
    public void setWriteMessageStore(MessageStore writeMessageStore) {
        this.writeMessageStore = writeMessageStore;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Map.Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    /**
     * 这里主要逻辑就是循环迭代对应的延迟级别缓存，然后根据不同的等级来获取对应的偏移量缓存。
     * 然后根据偏移量和延迟级创建一个DeliverDelayedMessageTimerTask进一步的处理。
     * 这里要说明的是offsetTable中存的是消息在ConsumeQueue中的偏移量。
     * start()方法在消息存储服务启动的时候会被调用
     * https://blog.csdn.net/weixin_37689658/article/details/122110461
     * 步骤：
     * 首先遍历delayLevelTable表，根据延迟级别从offsetTable中找到对应延迟队列的消费进度，
     * 然后给每一个延迟队列都创建一个TimeTask，并且把对应的延迟级别和延迟队列的消费进度传进TimeTask中，
     * 然后延迟1s执行。接着再创建一个定时任务每10s把每一个延迟队列的消费进度写入到delayOffset.json文件中
     */
    public void start() {
        //设置运行状态为开始
        if (started.compareAndSet(false, true)) {
            this.timer = new Timer("ScheduleMessageTimerThread", true);
            //迭代延迟级别的缓存
            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
                //获取等级和 延迟时间长度
                Integer level = entry.getKey();
                Long timeDelay = entry.getValue();
                //获取对应延迟级别的偏移量缓存，这里缓存的是ConsumeQueue文件中的消息的偏移量
                Long offset = this.offsetTable.get(level);
                //如果偏移量为null，说明没有消息需要处理，这设置为0
                if (null == offset) {
                    offset = 0L;
                }
                //如果延迟级别不为null，则构建DeliverDelayedMessageTimerTask任务进行处理
                if (timeDelay != null) {
                    /**
                     * schedule(TimerTask task, long delay)只执行一次，
                     * schedule(TimerTask task, long delay, long period)才是重复的执行(周期是period毫秒)
                     */
                    // 给每一个延迟级别都启动对应的TimeTask，延迟1s执行
                    this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME);
                }
            }

            /**
             * 对延迟队列的消费进度进行持久化
             * 每10s把每一个延迟队列的最大消息偏移量写入到磁盘中 delayOffset.json
             * 每10s把每一个延迟队列的消费进度写入到磁盘中，当在这10s内broker宕机了，在broker启动之后就会导致延迟消息的重复消费
             *
             */
            this.timer.scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    try {
                        /**
                         * 这个周期任务会每10s把offsetTable中的数据进行持久化到delayOffset.json文件，
                         * 这样下一次broker重启之后就可以从delayOffset.json中获取到上一次延迟队列的消费进度了
                         */
                        if (started.get()) ScheduleMessageService.this.persist();
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            if (null != this.timer)
                this.timer.cancel();
        }

    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public String encode() {
        return this.encode(false);
    }

    public boolean load() {
        /**
         * 调用父类的加载文件的方法，父类会调用子类实现的configFilePath方法确定文件
         * 即读取delayOffset.json文件的数据到offsetTable这个map中
         */
        boolean result = super.load();
        //加载成功则进行解析延迟级别配置 和初始化delayLevelTable表
        result = result && this.parseDelayLevel();
        return result;
    }

    @Override
    public String configFilePath() {
        //获取`delayOffset.json`文件
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    /**
     * 会去读取delayOffset.json文件的数据到offsetTable中
     * @param jsonString
     */
    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    /**
     * 解析延迟级别配置
     * gdtodo:我们可以通过修改配置文件（broker.conf）的方式来修改RocketMQ的最大延迟时间和对应的延迟级别。
     * 初始化delayLevelTable表
     * @return
     */
    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        // 默认为1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                // 计算真正的时长
                long delayTimeMillis = tu * num;
                // 保存到延迟级别缓存中
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    class DeliverDelayedMessageTimerTask extends TimerTask {
        private final int delayLevel;
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        @Override
        public void run() {
            try {
                if (isStarted()) {
                    //执行检查消息是否到时间的逻辑
                    this.executeOnTimeup();
                }
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                    this.delayLevel, this.offset), DELAY_FOR_A_PERIOD);
            }
        }

        /**
         * @return
         */
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {

            long result = deliverTimestamp;

            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }

            return result;
        }

        public void executeOnTimeup() {
            //根据 RMQ_SYS_SCHEDULE_TOPIC 和 延迟级别 找到对应的ConsumeQueue
            ConsumeQueue cq =
                ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                    delayLevel2QueueId(delayLevel));

            long failScheduleOffset = offset;

            if (cq != null) {
                //根据传入的  offset 从ConsumeQueue中获取对应的消息信息缓冲，这里获取到的不是真实的消息
                // 即根据offset得到对应的ConsumeQueue文件，然后再返回该文件从起始偏移量到写入位点的所有ConsumeQueueData  从offset到maxOffset
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                if (bufferCQ != null) {
                    try {
                        long nextOffset = offset;
                        int i = 0;
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        //从buffer中每次获取20个byte长度的信息，因为ConsumeQueue的存储单元大小为20byte
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            //获取消息在CommitLog中的真实位置
                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            //获取消息的大小
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            // 延迟结束时间，在消息写入到CommitLog之后会进行分发到ConsumeQueue，而对于延迟消息来说，tagCode这个位置存储的是该消息的延迟到期时间
                            long tagsCode = bufferCQ.getByteBuffer().getLong();

                            if (cq.isExtAddr(tagsCode)) {
                                if (cq.getExt(tagsCode, cqExtUnit)) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    //can't find ext content.So re compute tags code.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                                        tagsCode, offsetPy, sizePy);
                                    long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                                }
                            }

                            long now = System.currentTimeMillis();
                            /**
                             * 如果这个消息还未到达延迟结束时间，那么deliverTimestamp就是当前时间
                             * 反之如果这个消息已经到达了延迟结束时间，那么deliverTimestamp就是延迟结束时间
                             */
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                            // 这里计算出的偏移位是当前遍历的前一个ConsumeQueueData的位点，作用是当延迟消息投递到原始的topic失败的时候会根据这个偏移位点去重新执行投递
                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                            long countdown = deliverTimestamp - now;
                            //消息的延迟时间到了
                            if (countdown <= 0) {
                                //根据CommitLog物理偏移量找到msg
                                MessageExt msgExt =
                                    ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(
                                        offsetPy, sizePy);

                                if (msgExt != null) {
                                    try {
                                        // 拷贝一个新的msg对象，该msg对象的topic以及queueId都回到了原始的值，同时还会删除延迟级别（删除属性中"DELAY"的信息,即清除消息的delayLevel）
                                        MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                                        if (TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                                            log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}",
                                                    msgInner.getTopic(), msgInner);
                                            continue;
                                        }
                                        // 写入到CommitLog中
                                        PutMessageResult putMessageResult =
                                            ScheduleMessageService.this.writeMessageStore
                                                .putMessage(msgInner);

                                        // 写入成功，跳过该次循环，继续判断下一条延迟消息是否达到到期时间
                                        if (putMessageResult != null
                                            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                            continue;
                                        } else {
                                            // XXX: warn and notify me
                                            log.error(
                                                "ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}",
                                                msgExt.getTopic(), msgExt.getMsgId());
                                            ScheduleMessageService.this.timer.schedule(
                                                new DeliverDelayedMessageTimerTask(this.delayLevel,
                                                    nextOffset), DELAY_FOR_A_PERIOD);
                                            ScheduleMessageService.this.updateOffset(this.delayLevel,
                                                nextOffset);
                                            return;
                                        }
                                    } catch (Exception e) {
                                        /*
                                         * XXX: warn and notify me
                                         */
                                        log.error(
                                            "ScheduleMessageService, messageTimeup execute error, drop it. msgExt="
                                                + msgExt + ", nextOffset=" + nextOffset + ",offsetPy="
                                                + offsetPy + ",sizePy=" + sizePy, e);
                                    }
                                }
                            } else {
                                // 走到了else逻辑，说明此时队列中的延迟时间还未结束
                                /**
                                 * 重新提交一个TimeTask，并且设置的延迟执行时间为消息剩余的延时时间
                                 * gdtodo: 动态定时器
                                 * 消息未到达延迟结束时间的话，重新提交一个TimeTask，
                                 * 《延迟执行时间》为该当前时间与该消息延时结束时间的差值，---此处delay的设置达到了动态定时器的效果
                                 * 那么下一次该TimeTask执行的时候该消息就肯定已经到达了延迟结束时间了
                                 */
                                ScheduleMessageService.this.timer.schedule(
                                    new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset),
                                    countdown);
                                /**
                                 * 更新这个延迟队列的本地缓存消费进度（即更新延迟队列已消费的消息偏移量）
                                 */
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        } // end of for

                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                            this.delayLevel, nextOffset), DELAY_FOR_A_WHILE);
                        /**
                         * 更新这个延迟队列的本地缓存消费进度（即更新延迟队列已消费的消息偏移量）,
                         * 其实executeOnTimeup方法中有好几处updateOffset,保证了只要有延迟消息被消费了，就会更新offsetTable中这个延迟队列的消费进度
                         */
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    } finally {

                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
                else {

                    long cqMinOffset = cq.getMinOffsetInQueue();
                    if (offset < cqMinOffset) {
                        failScheduleOffset = cqMinOffset;
                        log.error("schedule CQ offset invalid. offset=" + offset + ", cqMinOffset="
                            + cqMinOffset + ", queueId=" + cq.getQueueId());
                    }
                }
            } // end of if (cq != null)

            // 代码执行到这里说明ConsumeQueue == null，延迟100ms继续执行该任务
            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel,
                failScheduleOffset), DELAY_FOR_A_WHILE);
        }

        private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            // 从msg的properties的"TAGS"中获取tags了
            long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);
            // 删除属性中"DELAY"的信息，即删除延迟级别，清除消息的delayLevel 即清除延时信息属性 PROPERTY_DELAY_TIME_LEVEL
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
            // 从msg的properties的"REAL_TOPIC"中获取topic了
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }
    }
}
