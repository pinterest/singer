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
package com.pinterest.singer.writer;

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.LogStreamWriter;
import com.pinterest.singer.common.errors.LogStreamWriterException;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.AuditMessage;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.thrift.configuration.SingerRestartConfig;
import com.pinterest.singer.utils.PartitionComparator;
import com.pinterest.singer.utils.SingerUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * LogStreamWriter implementation that write to Kafka cluster.
 * <p/>
 * This class is NOT thread-safe.
 */
public class KafkaWriter implements LogStreamWriter {

  public static final String HOSTNAME = SingerSettings.getEnvironment().getHostname();
  private static final Logger LOG = LoggerFactory.getLogger(KafkaWriter.class);
  private static final PartitionComparator COMPARATOR = new PartitionComparator();

  // Counter for the number of batch message writing failures
  private static AtomicInteger failureCounter = new AtomicInteger(0);

  private final LogStream logStream;

  // Topic to which this LogWriter writes.
  private final String topic;

  private final String logName;

  private final boolean skipNoLeaderPartitions;

  private final boolean auditingEnabled;

  private final String auditTopic;

  private final KafkaProducerConfig producerConfig;

  private final String kafkaClusterSig;

  private final ExecutorService clusterThreadPool;

  private final TSerializer serializer;

  private final int writeTimeoutInSeconds;

  private KafkaMessagePartitioner partitioner;

  public KafkaWriter(LogStream logStream,
      KafkaProducerConfig producerConfig,
      String topic,
      boolean skipNoLeaderPartitions,
      boolean auditingEnabled,
      String auditTopic,
      String partitionerClassName,
      int writeTimeoutInSeconds) throws Exception {
    Preconditions.checkNotNull(logStream);
    Preconditions.checkNotNull(producerConfig);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(topic));

    this.logStream = logStream;
    this.logName = logStream.getSingerLog().getSingerLogConfig().getName();
    this.topic = topic;
    this.skipNoLeaderPartitions = skipNoLeaderPartitions;
    this.auditingEnabled = auditingEnabled && !Strings.isNullOrEmpty(auditTopic);
    this.auditTopic = auditTopic;
    this.producerConfig = producerConfig;
    // cache the class and instance instead of doing reflections each time 
    try {
      @SuppressWarnings("unchecked")
      Class<KafkaMessagePartitioner> partitionerClass 
            = (Class<KafkaMessagePartitioner>) Class.forName(partitionerClassName);
      partitioner = partitionerClass.getConstructor().newInstance();
    } catch (Exception e) {
      LOG.error("Invalid partitioner configuration", e);
      throw e;
    }
    this.kafkaClusterSig = producerConfig.getKafkaClusterSignature();
    this.clusterThreadPool = SingerSettings.getLogWritingExecutors().get(kafkaClusterSig);
    this.writeTimeoutInSeconds = writeTimeoutInSeconds;
    this.serializer = new TSerializer();
  }

  protected KafkaWriter(KafkaProducerConfig producerConfig,
                        KafkaMessagePartitioner partitioner,
                        String topic,
                        boolean skipNoLeaderPartitions,
                        ExecutorService clusterThreadPool) {
    this.partitioner = partitioner;
    this.skipNoLeaderPartitions = skipNoLeaderPartitions;
    this.producerConfig = producerConfig;
    this.clusterThreadPool = clusterThreadPool;
    this.topic = topic;
    logStream = null;
    logName = null;
    writeTimeoutInSeconds = 0;
    serializer = null;
    auditingEnabled = false;
    auditTopic = null;
    kafkaClusterSig = null;
  }

  @Override
  public LogStream getLogStream() {
    return logStream;
  }

  @Override
  public boolean isAuditingEnabled() {
    return this.auditingEnabled;
  }

  private void includeAuditMessageInBatch(List<ProducerRecord<byte[], byte[]>> messages) {
    AuditMessage auditMessage = new AuditMessage();
    auditMessage.setTimestamp(System.currentTimeMillis());
    auditMessage.setHostname(HOSTNAME);
    auditMessage.setTopic(topic);
    auditMessage.setNumMessages(messages.size());

    try {
      ProducerRecord<byte[], byte[]> keyedMessage;
      byte[] auditBytes = serializer.serialize(auditMessage);
      keyedMessage = new ProducerRecord<>(auditTopic, auditBytes);
      messages.add(keyedMessage);
    } catch (Exception e) {
      LOG.error("Failed to include audit message: ", e);
    }
  }

  /**
   * Distribute the message batch into different partitions, and use the under-lying kafka writing thread pool to speed
   * up writing.
   *
   * If skipNoLeaderPartitions flag is set, we will skip the partitions that have no leader.
   *
   * @param partitions unordered list of partitionInfo to be used for partitioning this batch
   * @param topic the kafka topic
   * @param logMessages the messages that will be written to kafka
   * @return a list of message lists that are classified based on partitions.
   */
  List<List<ProducerRecord<byte[], byte[]>>> messageCollation(
      List<PartitionInfo> partitions,
      String topic,
      List<LogMessage> logMessages) throws Exception {
    LOG.info("Collate " + logMessages.size() + " messages");

    List<List<ProducerRecord<byte[], byte[]>>> buckets = new ArrayList<>();
    try {
      List<PartitionInfo> validPartitions = partitions;
      if (skipNoLeaderPartitions) {
        validPartitions = new ArrayList<>();
        for (PartitionInfo partitionInfo : partitions) {
          // If there is no leader, the id value is -1
          // github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/PartitionInfo.java
          if (partitionInfo.leader().id() >= 0) {
            validPartitions.add(partitionInfo);
          }
        }
      }

      ProducerRecord<byte[], byte[]> keyedMessage;
      for (int i = 0; i < validPartitions.size(); i++) {
        buckets.add(new ArrayList<>());
      }

      for (LogMessage msg : logMessages) {
        byte[] key = null;
        if (msg.isSetKey()) {
          key = SingerUtils.readFromByteBuffer(msg.BufferForKey());
        }
        int partitionId = partitioner.partition(key, validPartitions);
        if (skipNoLeaderPartitions) {
          partitionId = validPartitions.get(partitionId).partition();
        }
        keyedMessage = new ProducerRecord<>(topic, partitionId, key, SingerUtils.readFromByteBuffer(msg.BufferForMessage()));
        buckets.get(partitionId).add(keyedMessage);
      }
    } catch (Exception e) {
      LOG.error("Failed in message collation for topic {}, partitioner {}", topic, partitioner.getClass().getName(), e);
      throw new LogStreamWriterException(e.toString());
    }
    return buckets;
  }

  @Override
  public void writeLogMessages(List<LogMessage> logMessages) throws LogStreamWriterException {
    Set<Future<KafkaWritingTaskResult>> resultSet = new HashSet<>();
    KafkaProducer<byte[], byte[]> producer = KafkaProducerManager.getProducer(producerConfig);
    Preconditions.checkNotNull(producer);

    if (producerConfig.isTransactionEnabled()) {
      producer.beginTransaction();
    }
    try {
      List<PartitionInfo> partitions = producer.partitionsFor(topic);
      List<List<ProducerRecord<byte[], byte[]>>>
          buckets = messageCollation(partitions, topic, logMessages);
      
      // we sort this info after, we have to create a copy of the data since
      // the returned list is immutable
      List<PartitionInfo> sortedPartitions = new ArrayList<>(partitions);
      Collections.sort(sortedPartitions, COMPARATOR);

      for (List<ProducerRecord<byte[], byte[]>> msgs : buckets) {
        if (msgs.size() > 0) {
          if (auditingEnabled) {
            includeAuditMessageInBatch(msgs);
          }
          Callable<KafkaWritingTaskResult> worker =
              new KafkaWritingTask(producer, msgs, writeTimeoutInSeconds, sortedPartitions);
          Future<KafkaWritingTaskResult> future = clusterThreadPool.submit(worker);
          resultSet.add(future);
        }
      }

      int bytesWritten = 0;
      int maxKafkaBatchWriteLatency = 0;
      for (Future<KafkaWritingTaskResult> f : resultSet) {
        KafkaWritingTaskResult result = f.get();
        if (!result.success) {
          LOG.error("Failed to write messages to kafka topic {}", topic, result.exception);
          throw new LogStreamWriterException("Failed to write messages to kafka");
        } else {
          bytesWritten += result.getWrittenBytesSize();
          // get the max write latency
          maxKafkaBatchWriteLatency = Math.max(maxKafkaBatchWriteLatency,
              result.getKafkaBatchWriteLatencyInMillis());
        }
      }
      if (producerConfig.isTransactionEnabled()) {
        producer.commitTransaction();
        OpenTsdbMetricConverter.incr(SingerMetrics.NUM_COMMITED_TRANSACTIONS, 1,
            "topic=" + topic, "host=" + HOSTNAME, "logname=" + logName);
      }
      OpenTsdbMetricConverter.gauge(SingerMetrics.KAFKA_THROUGHPUT, bytesWritten,
          "topic=" + topic, "host=" + HOSTNAME, "logname=" + logName);
      OpenTsdbMetricConverter.gauge(SingerMetrics.KAFKA_LATENCY, maxKafkaBatchWriteLatency,
          "topic=" + topic, "host=" + HOSTNAME, "logname=" + logName);
      OpenTsdbMetricConverter.incr(SingerMetrics.NUM_KAFKA_MESSAGES, logMessages.size(),
          "topic=" + topic, "host=" + HOSTNAME, "logname=" + logName);
    } catch (Exception e) {
      LOG.error("Caught exception when write " + logMessages.size() + " messages to producer.", e);

      SingerRestartConfig restartConfig = SingerSettings.getSingerConfig().singerRestartConfig;
      if (restartConfig != null && restartConfig.restartOnFailures
          && failureCounter.incrementAndGet() > restartConfig.numOfFailuesAllowed) {
        LOG.error("Encountered {} kafka logging failures.", failureCounter.get());
      }
      if (producerConfig.isTransactionEnabled()) {
        producer.abortTransaction();
        OpenTsdbMetricConverter.incr(SingerMetrics.NUM_ABORTED_TRANSACTIONS, 1,
            "topic=" + topic, "host=" + HOSTNAME, "logname=" + logName);
      }
      KafkaProducerManager.resetProducer(producerConfig);
      OpenTsdbMetricConverter.incr("singer.writer.producer_reset", 1,
          "topic=" + topic, "host=" + HOSTNAME);
      OpenTsdbMetricConverter.incr("singer.writer.num_kafka_messages_delivery_failure",
          logMessages.size(), "topic=" + topic, "host=" + HOSTNAME, "logname=" + logName);

      throw new LogStreamWriterException("Failed to write messages to topic " + topic, e);
    } finally {
      for (Future<KafkaWritingTaskResult> f : resultSet) {
        if (!f.isDone() && !f.isCancelled()) {
          f.cancel(true);
        }
      }
    }
  }

  public void close() throws IOException {
    // We should not close the producer here since the producer might be shared by multiple
    // LogStreamWriter.
  }
}
