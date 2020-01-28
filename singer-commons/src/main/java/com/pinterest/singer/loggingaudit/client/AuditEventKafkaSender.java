/**
 * Copyright 2019 Pinterest, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.singer.loggingaudit.client;

import com.pinterest.singer.loggingaudit.client.common.LoggingAuditClientMetrics;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditHeaders;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditEvent;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditStage;
import com.pinterest.singer.loggingaudit.thrift.configuration.KafkaSenderConfig;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.utils.CommonUtils;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *  LoggingAuditEventSender implementations that dequeue LoggingAuditEvent and send it to Kafka.
 *
 *  Each instance of this class should be executed in its own thread.
 */
public class AuditEventKafkaSender implements LoggingAuditEventSender {

  private static final Logger LOG = LoggerFactory.getLogger(AuditEventKafkaSender.class);

  private static final int MAX_RETRIES_FOR_SELECTION_RANDOM_PARTITION = 10;

  private static final int PARTITIONS_REFRESH_INTERVAL_IN_SECONDS = 30;

  /**
   *  When sender send audit events to Kafka,  it chooses a random partition and if it fails, it
   *  will choose another GOOD partition, this retry will happen at most numOfPartitionsToTrySending
   *  times before dropping the event. Note that, this is different from the retry handled by
   *  kafka client
   *  library when sending an event to a certain partition.
   */
  private static final int NUM_OF_PARTITIONS_TO_TRY_SENDING = 3;

  /**
   *  maximum time to wait when sender tries to dequeue events before returning null.
   */
  private static final int DEQUEUE_WAIT_IN_SECONDS = 30;


  /**
   *   when gracefully shutting down the Sender, the calling thread sleeps for some time and let
   *   the sender to send out audit events left in the queue if there are any.
   *   stopGracePeriodInSeconds is the maximum time reserved and
   *   stopGracePeriodInSeconds / THREAD_SLEEP_IN_SECONDS is the total rounds the calling thread
   *   will sleep.
   */
  private static final int THREAD_SLEEP_IN_SECONDS = 10;

  /**
   *  when gracefully shutting down the Sender, this field specifies maximum time for main thread
   *  to wait, in order to let the sender send out audit events left in the queue if there are any.
   */
  private int stopGracePeriodInSeconds = 300;


  /**
   * Logging audit stage, can be THRIFTLOGGER, SINGER, MERCED and so on
   */
  private final LoggingAuditStage stage;

  /**
   * host name
   */
  private final String host;

  /**
   *  LinkedBlockingDequeue to store the LoggingAuditEvents.  The max capacity is specified when
   *  creating this deque in the LoggingAuditClient.java
   *
   *  The AuditEventKafkaSender dequeue from the beginning of the deque, if does not send out event
   *  successfully, it will enqueue this event to the beginning of the queue.  Note that, if enqueue
   *  the failed event at the end of the queue, this event could be processed with quite some delay
   *  and this is not the behavior we want.  That's we use LinkedBlockingDequeue,
   *  not ArrayBlockingQueue.
   */
  private final LinkedBlockingDeque<LoggingAuditEvent> queue;

  /**
   * KafkaProducer instance to send audit events
   */
  private KafkaProducer<byte[], byte[]> kafkaProducer;

  /**
   * Serialize key and value to byte[]
   */
  private TSerializer serializer = new TSerializer();

  /**
   *  flag to control the start and stop of the executing thread.
   */
  private AtomicBoolean cancelled = new AtomicBoolean(false);

  /**
   *  topic to store the audit events
   */
  private String topic;

  /**
   *  name of this sender instance
   */
  private String name;

  /**
   *  executing thread
   */
  private Thread thread;

  /**
   *  For each partition, track the number of sending failures happened to this partition.
   */
  private List<PartitionInfo> partitionInfoList = new ArrayList<>();

  /**
   *  last time when partition list was refreshed. we want to refresh partition list every 5 mins.
   */
  private long lastTimeUpdate = -1;

  /**
   *  If sending out to one partition fails, this partition is added to set;
   *  If sending out to one partition succeeds, this partition is removed if it was added before.
   */
  private Set<Integer> badPartitions = ConcurrentHashMap.newKeySet();

  /**
   * For each event (identified by LoggingAuditHeaders, key of the map), track the number of tries
   * for sending to Kafka. Each try will choose a different and partition that is not in the
   * badPartitions.  When event is send out successfully or dropped, the corresponding entry in
   * this map is removed.
   */

  private Map<LoggingAuditHeaders, Integer> eventTriedCount = new ConcurrentHashMap<>();

  public AuditEventKafkaSender(KafkaSenderConfig config,
                               LinkedBlockingDeque<LoggingAuditEvent> queue,
                               LoggingAuditStage stage, String host, String name) {
    this.topic = config.getTopic();
    this.queue = queue;
    this.stage = stage;
    this.host = host;
    this.name = name;
    this.stopGracePeriodInSeconds = config.getStopGracePeriodInSeconds();
  }


  public KafkaProducer<byte[], byte[]> getKafkaProducer() {
    return kafkaProducer;
  }

  public void setKafkaProducer(KafkaProducer<byte[], byte[]> kafkaProducer) {
    this.kafkaProducer = kafkaProducer;
  }

  public int getAlternatePartition(int numOfPartitions) {
    int randomPartition = 0;
    int trial = 0;
    while (trial < MAX_RETRIES_FOR_SELECTION_RANDOM_PARTITION) {
      trial += 1;
      randomPartition = ThreadLocalRandom.current().nextInt(numOfPartitions);
      if (!badPartitions.contains(randomPartition)) {
        break;
      }
    }
    return randomPartition;
  }

  private void refreshPartitionIfNeeded() {
    // refresh every 30 seconds
    if (System.currentTimeMillis() - lastTimeUpdate > 1000 * PARTITIONS_REFRESH_INTERVAL_IN_SECONDS) {
      try {
        badPartitions.clear();
        partitionInfoList = this.kafkaProducer.partitionsFor(topic);
        lastTimeUpdate = System.currentTimeMillis();
        OpenTsdbMetricConverter.incr(
            LoggingAuditClientMetrics.AUDIT_CLIENT_SENDER_KAFKA_PARTITIONS_REFRESH_COUNT, 1,
                "host=" + host, "stage=" + stage.toString());
      } catch (Exception e) {
        OpenTsdbMetricConverter.incr(
            LoggingAuditClientMetrics.AUDIT_CLIENT_SENDER_KAFKA_PARTITIONS_REFRESH_ERROR, 1,
                "host=" + host, "stage=" + stage.toString());
      }
    }
  }

  /**
   *  Sender dequeues LoggingAuditEvents and sends them to Kafka cluster. If send to one partition
   *  fails, it will choose another partition. For each event, it will try at most
   *  NUM_OF_PARTITIONS_TO_TRY_SENDING times (3 times) before dropping this event. Metrics are
   *  used to track the queue size and usuage, number of events sent out to Kafka successfully, and
   *  the number of events dropped.
   */
  @Override
  public void run() {
    LoggingAuditEvent event = null;
    ProducerRecord<byte[], byte[]> record;
    byte[] value = null;

    while (!cancelled.get()) {
      try {
        refreshPartitionIfNeeded();
        event = queue.poll(DEQUEUE_WAIT_IN_SECONDS, TimeUnit.SECONDS);
        if (event != null) {
          try {
            value = serializer.serialize(event);
            record = new ProducerRecord<>(this.topic, getAlternatePartition(
                partitionInfoList.size()), null, value);
            kafkaProducer.send(record, new KafkaProducerCallback(event));
          } catch (TException e) {
            LOG.debug("[{}] failed to construct ProducerRecord because of serialization exception.",
                Thread.currentThread().getName(), e);
            OpenTsdbMetricConverter
                .incr(LoggingAuditClientMetrics.AUDIT_CLIENT_SENDER_SERIALIZATION_EXCEPTION, 1,
                    "host=" + host, "stage=" + stage.toString(),
                    "logName=" + event.getLoggingAuditHeaders().getLogName());
            eventTriedCount.remove(event.getLoggingAuditHeaders());
          }
        }
      } catch (InterruptedException e) {
        LOG.warn("[{}] got interrupted when polling the queue and while loop is ended!",
            Thread.currentThread().getName(), e);
        OpenTsdbMetricConverter.incr(
            LoggingAuditClientMetrics.AUDIT_CLIENT_SENDER_DEQUEUE_INTERRUPTED_EXCEPTION, 1,
                "host=" + host, "stage=" + stage.toString());
        break;
      } catch (Exception e) {
        LOG.warn("Exit the while loop and finish the thread execution due to exception: ", e);
        OpenTsdbMetricConverter.incr(LoggingAuditClientMetrics.AUDIT_CLIENT_SENDER_EXCEPTION, 1,
            "host=" + host, "stage=" + stage.toString());
        break;
      }
    }
  }


  public class KafkaProducerCallback implements Callback {

    private LoggingAuditEvent event;

    public KafkaProducerCallback(LoggingAuditEvent event) {
      this.event = event;
    }

    public void checkAndEnqueueWhenSendFailed(RecordMetadata recordMetadata, Exception e) {
      // if exception thrown (i.e. the send failed), the partition is added to badPartitions.
      badPartitions.add(recordMetadata.partition());
      OpenTsdbMetricConverter
          .incr(LoggingAuditClientMetrics.AUDIT_CLIENT_SENDER_KAFKA_PARTITION_ERROR, 1,
              "host=" + host, "stage=" + stage.toString(), "topic=" + topic,
              "partition=" + recordMetadata.partition());

      // retry the failed event by inserting it at the beginning of the deque.
      // If number of tries reaches 3, meaning that 3 partitions have been tried sending to but
      // still failed, this event is dropped.
      Integer count = eventTriedCount.get(event.getLoggingAuditHeaders());
      if (count == null){
        eventTriedCount.put(event.getLoggingAuditHeaders(), 1);
        insertEvent(event);
        OpenTsdbMetricConverter
            .gauge(LoggingAuditClientMetrics.AUDIT_CLIENT_SENDER_KAFKA_EVENTS_RETRIED,
                eventTriedCount.size(), "host=" + host, "stage=" + stage.toString(),
                "topic=" + topic);
      } else if (count >= NUM_OF_PARTITIONS_TO_TRY_SENDING) {
          LOG.debug("Failed to send audit event after trying {} partitions. Drop event.", count);
          OpenTsdbMetricConverter
              .incr(LoggingAuditClientMetrics.AUDIT_CLIENT_SENDER_KAFKA_EVENTS_DROPPED, 1,
                  "host=" + host, "stage=" + stage.toString(),
                  "logName=" + event.getLoggingAuditHeaders().getLogName());
          eventTriedCount.remove(event.getLoggingAuditHeaders());
      } else {
          eventTriedCount.put(event.getLoggingAuditHeaders(), count + 1);
          insertEvent(event);
      }
    }

    public void insertEvent(LoggingAuditEvent event){
      try {
        boolean success = queue.offerFirst(event, 3, TimeUnit.SECONDS);
        if (!success) {
          LOG.debug("Failed to enqueue LoggingAuditEvent at head of the queue when executing "
              + "producer send callback. Drop this event.");
          eventTriedCount.remove(event.getLoggingAuditHeaders());
        }
      } catch (InterruptedException ex) {
        LOG.debug(
            "Enqueuing LoggingAuditEvent at head of the queue was interrupted in callback. "
                + "Drop this event");
        eventTriedCount.remove(event.getLoggingAuditHeaders());
      }
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      try {
        if (e == null) {
          OpenTsdbMetricConverter
              .incr(LoggingAuditClientMetrics.AUDIT_CLIENT_SENDER_KAFKA_EVENTS_ACKED, 1,
                  "host=" + host, "stage=" + stage.toString(),
                  "logName=" + event.getLoggingAuditHeaders().getLogName());

          // if send is successful, remove the event from the map eventTriedCount if it was added
          // LoggingAuditHeaders can uniquely identify an event.
          eventTriedCount.remove(event.getLoggingAuditHeaders());
          // if send out successfully, remove the partition from the badPartitions if it was added.
          badPartitions.remove(recordMetadata.partition());
        } else {
          checkAndEnqueueWhenSendFailed(recordMetadata, e);
        }
      } catch (Throwable t) {
        LOG.warn("Exception throws in the callback. Drop this event {}", event, t);
        OpenTsdbMetricConverter
            .incr(LoggingAuditClientMetrics.AUDIT_CLIENT_SENDER_KAFKA_CALLBACK_EXCEPTION, 1,
                "host=" + host, "stage=" + stage.toString(), "topic=" + topic);
      }
    }
  }

  /**
   *  start the executing thread and let the Sender run.
   */
  public synchronized void start() {
    if (this.thread == null) {
      thread = new Thread(this);
      thread.setDaemon(true);
      thread.setName(name);
      thread.start();
      LOG.warn(
          "[{}] created and started [{}] to let it dequeue LoggingAuditEvents and send to Kafka.",
          Thread.currentThread().getName(), name);
    }
  }

  /**
   *  reserve some time (by default 30 seconds at most)to let AuditEventKafkaSender to send out
   *  LoggingAuditEvent in the queue and gracefully stop AuditEventKafkaSender.
   */
  public synchronized void stop() {
    LOG.warn(
        "[{}] waits up to {} seconds to let [{}] send out LoggingAuditEvents left in the queue if"
            + " any.",
        Thread.currentThread().getName(), stopGracePeriodInSeconds, name);
    int i = 0;
    int numOfRounds = stopGracePeriodInSeconds / THREAD_SLEEP_IN_SECONDS;
    while (queue.size() > 0 && this.thread != null && thread.isAlive() && i < numOfRounds) {
      i += 1;
      try {
        Thread.sleep(THREAD_SLEEP_IN_SECONDS * 1000);
        CommonUtils.reportQueueUsage(queue.size(), queue.remainingCapacity(), host, stage.toString());
        LOG.info("In {} round, [{}] waited {} seconds and the current queue size is {}", i,
            Thread.currentThread().getName(), THREAD_SLEEP_IN_SECONDS, queue.size());
      } catch (InterruptedException e) {
        LOG.warn("[{}] got interrupted while waiting for [{}] to send out LoggingAuditEvents left "
            + "in the queue.", Thread.currentThread().getName(), name, e);
      }
    }
    cancelled.set(true);
    if (this.thread != null && thread.isAlive()) {
      thread.interrupt();
    }
    try {
      this.kafkaProducer.close();
    } catch (Throwable t) {
      LOG.warn("Exception is thrown while stopping {}.", name, t);
    }
    LOG.warn("[{}] is stopped and the number of LoggingAuditEvents left in the queue is {}.", name,
        queue.size());
  }

}
