/**
 * Copyright 2020 Pinterest, Inc.
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
package com.pinterest.singer.writer.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.LogStreamWriter;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.common.errors.LogStreamWriterException;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditHeaders;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.LogMessageAndPosition;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.thrift.configuration.SingerRestartConfig;
import com.pinterest.singer.writer.KafkaMessagePartitioner;
import com.pinterest.singer.writer.KafkaProducerManager;
import com.pinterest.singer.writer.KafkaWriter;
import com.pinterest.singer.writer.KafkaWritingTaskResult;

/**
 * Committable writer that implements the commit design pattern methods of {@link LogStreamWriter}
 * 
 * This class allows usage of MemoryEfficientLogStreamProcessor.
 */
public class CommittableKafkaWriter extends KafkaWriter {

  private static final Logger LOG = LoggerFactory.getLogger(CommittableKafkaWriter.class);
  private List<PartitionInfo> committableValidPartitions;
  private Map<Integer, Map<Integer, LoggingAuditHeaders>> committableMapOfTrackedMessageMaps;
  private Map<Integer, Map<Integer, LoggingAuditHeaders>> committableMapOfInvalidMessageMaps;
  private Map<Integer, Integer> committableMapOfOriginalIndexWithinBucket;
  private Map<Integer, KafkaWritingTaskFuture> commitableBuckets;
  private KafkaProducer<byte[], byte[]> committableProducer;

  protected CommittableKafkaWriter(KafkaProducerConfig producerConfig,
                                   KafkaMessagePartitioner partitioner,
                                   String topic,
                                   boolean skipNoLeaderPartitions,
                                   ExecutorService clusterThreadPool) {
    super(producerConfig, partitioner, topic, skipNoLeaderPartitions, clusterThreadPool);
  }

  public CommittableKafkaWriter(LogStream logStream,
                                KafkaProducerConfig producerConfig,
                                KafkaMessagePartitioner partitioner,
                                String topic,
                                boolean skipNoLeaderPartitions,
                                ExecutorService clusterThreadPool,
                                boolean enableHeadersInjector) {
    super(logStream, producerConfig, partitioner, topic, skipNoLeaderPartitions, clusterThreadPool,
        enableHeadersInjector);
  }

  public CommittableKafkaWriter(LogStream logStream,
                                KafkaProducerConfig producerConfig,
                                String topic,
                                boolean skipNoLeaderPartitions,
                                boolean auditingEnabled,
                                String auditTopic,
                                String partitionerClassName,
                                int writeTimeoutInSeconds,
                                boolean enableHeadersInjector) throws Exception {
    super(logStream, producerConfig, topic, skipNoLeaderPartitions, auditingEnabled, auditTopic,
        partitionerClassName, writeTimeoutInSeconds, enableHeadersInjector);
    LOG.info("Enabled committablewriter for:" + topic);
  }

  public CommittableKafkaWriter(LogStream logStream,
                                KafkaProducerConfig producerConfig,
                                String topic,
                                boolean skipNoLeaderPartitions,
                                boolean auditingEnabled,
                                String auditTopic,
                                String partitionerClassName,
                                int writeTimeoutInSeconds) throws Exception {
    super(logStream, producerConfig, topic, skipNoLeaderPartitions, auditingEnabled, auditTopic,
        partitionerClassName, writeTimeoutInSeconds);
  }

  @Override
  public void startCommit() throws LogStreamWriterException {
    committableProducer = KafkaProducerManager.getProducer(producerConfig);
    Preconditions.checkNotNull(committableProducer);
    if (producerConfig.isTransactionEnabled()) {
      committableProducer.beginTransaction();
    }
    List<PartitionInfo> partitions = committableProducer.partitionsFor(topic);
    List<PartitionInfo> committableSortedPartitions = new ArrayList<>(partitions);
    Collections.sort(committableSortedPartitions, COMPARATOR);

    committableValidPartitions = partitions;
    if (skipNoLeaderPartitions) {
      committableValidPartitions = new ArrayList<>();
      for (PartitionInfo partitionInfo : partitions) {
        // If there is no leader, the id value is -1
        // github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/PartitionInfo.java
        if (partitionInfo.leader().id() >= 0) {
          committableValidPartitions.add(partitionInfo);
        }
      }
    }

    commitableBuckets = new HashMap<>();
    committableMapOfTrackedMessageMaps = new HashMap<>();
    committableMapOfInvalidMessageMaps = new HashMap<>();
    committableMapOfOriginalIndexWithinBucket = new HashMap<>();


    for (int i = 0; i < committableValidPartitions.size(); i++) {
      // for each partitionId, there is a corresponding bucket in buckets and a
      // corresponding headersMap in mapOfHeadersMaps.
      PartitionInfo partitionInfo = committableValidPartitions.get(i);
      int partitionId = partitionInfo.partition();
      commitableBuckets.put(partitionId, new KafkaWritingTaskFuture(partitionInfo));
      committableMapOfTrackedMessageMaps.put(partitionId, new HashMap<Integer, LoggingAuditHeaders>());
      committableMapOfInvalidMessageMaps.put(partitionId, new HashMap<Integer, LoggingAuditHeaders>());
      committableMapOfOriginalIndexWithinBucket.put(partitionId, -1);
    }
  }

  @Override
  public void writeLogMessageToCommit(LogMessageAndPosition message) throws LogStreamWriterException {
    LogMessage msg = message.getLogMessage();
    ProducerRecord<byte[], byte[]> keyedMessage;
    byte[] key = null;
    if (msg.isSetKey()) {
      key = msg.getKey();
    }
    int partitionId = partitioner.partition(key, committableValidPartitions);
    if (skipNoLeaderPartitions) {
      partitionId = committableValidPartitions.get(partitionId).partition();
    }
    keyedMessage = new ProducerRecord<>(topic, partitionId, key, msg.getMessage());
    Headers headers = keyedMessage.headers();
    checkAndSetLoggingAuditHeadersForLogMessage(msg);
    committableMapOfOriginalIndexWithinBucket.put(partitionId, 1 + committableMapOfOriginalIndexWithinBucket.get(partitionId));
    if (msg.getLoggingAuditHeaders() != null) {
      // check if the message should be skipped
      if (checkMessageValidAndInjectHeaders(msg, headers, committableMapOfOriginalIndexWithinBucket.get(partitionId), partitionId,
          committableMapOfTrackedMessageMaps, committableMapOfInvalidMessageMaps)) {
        return;
      }
    }

    KafkaWritingTaskFuture kafkaWritingTaskFutureResult = commitableBuckets.get(partitionId);
    List<Future<RecordMetadata>> recordMetadataList = kafkaWritingTaskFutureResult
        .getRecordMetadataList();

    if (recordMetadataList.isEmpty()) {
      kafkaWritingTaskFutureResult.setFirstProduceTimestamp(System.currentTimeMillis());
    }

    Future<RecordMetadata> send = committableProducer.send(keyedMessage);
    recordMetadataList.add(send);
  }

  @Override
  public void endCommit(int numLogMessages) throws LogStreamWriterException {
    committableProducer.flush();
    List<Future<KafkaWritingTaskResult>> resultFutures = new ArrayList<>();
    for (Entry<Integer, KafkaWritingTaskFuture> entry : commitableBuckets.entrySet()) {
      Future<KafkaWritingTaskResult> future = clusterThreadPool.submit(new KafkaWriteTask(entry));
      resultFutures.add(future);
    }

    int bytesWritten = 0;
    int maxKafkaBatchWriteLatency = 0;
    boolean anyBucketSendFailed = false;
    try {
      for (Future<KafkaWritingTaskResult> f : resultFutures) {
        KafkaWritingTaskResult result = f.get();
        if (!result.success) {
          LOG.error("Failed to write messages to kafka topic {}", topic, result.exception);
          anyBucketSendFailed = true;
        } else {
          bytesWritten += result.getWrittenBytesSize();
          // get the max write latency
          maxKafkaBatchWriteLatency = Math.max(maxKafkaBatchWriteLatency,
              result.getKafkaBatchWriteLatencyInMillis());
          if (isLoggingAuditEnabledAndConfigured()) {
            captureAndLogAuditEvents(result);
          }
        }
      }
      if (anyBucketSendFailed) {
        throw new LogStreamWriterException("Failed to write messages to kafka");
      }

      if (producerConfig.isTransactionEnabled()) {
        committableProducer.commitTransaction();
        OpenTsdbMetricConverter.incr(SingerMetrics.NUM_COMMITED_TRANSACTIONS, 1, "topic=" + topic,
            "host=" + HOSTNAME, "logname=" + logName);
      }
      updateWriteSuccessMetrics(numLogMessages, bytesWritten, maxKafkaBatchWriteLatency);
    } catch (Exception e) {
      LOG.error("Caught exception when write " + numLogMessages + " messages to producer.", e);

      SingerRestartConfig restartConfig = SingerSettings.getSingerConfig().singerRestartConfig;
      if (restartConfig != null && restartConfig.restartOnFailures
          && failureCounter.incrementAndGet() > restartConfig.numOfFailuesAllowed) {
        LOG.error("Encountered {} kafka logging failures.", failureCounter.get());
      }
      if (producerConfig.isTransactionEnabled()) {
        committableProducer.abortTransaction();
        OpenTsdbMetricConverter.incr(SingerMetrics.NUM_ABORTED_TRANSACTIONS, 1, "topic=" + topic,
            "host=" + HOSTNAME, "logname=" + logName);
      }
      KafkaProducerManager.resetProducer(producerConfig);
      updateWriteFailureMetrics(numLogMessages);
      throw new LogStreamWriterException("Failed to write messages to topic " + topic, e);
    } finally {
      for (Future<KafkaWritingTaskResult> f : resultFutures) {
        if (!f.isDone() && !f.isCancelled()) {
          f.cancel(true);
        }
      }
    }
  }

  private void captureAndLogAuditEvents(KafkaWritingTaskResult result) {
    if (isLoggingAuditEnabledAndConfigured()) {
      int bucketIndex = result.getPartition();
      enqueueLoggingAuditEvents(result, committableMapOfTrackedMessageMaps.get(bucketIndex),
          committableMapOfInvalidMessageMaps.get(bucketIndex));
    }
  }

  private void updateWriteFailureMetrics(int numLogMessages) {
    OpenTsdbMetricConverter.incr("singer.writer.producer_reset", 1, "topic=" + topic,
        "host=" + HOSTNAME);
    OpenTsdbMetricConverter.incr("singer.writer.num_kafka_messages_delivery_failure",
        numLogMessages, "topic=" + topic, "host=" + HOSTNAME, "logname=" + logName);
    OpenTsdbMetricConverter.incr(SingerMetrics.SINGER_WRITER 
        + "num_committable_kafka_messages_delivery_failure", numLogMessages,
        "topic=" + topic, "host=" + HOSTNAME, "logname=" + logName);
  }

  private void updateWriteSuccessMetrics(int numLogMessages, int bytesWritten, int maxKafkaBatchWriteLatency) {
    OpenTsdbMetricConverter.gauge(SingerMetrics.KAFKA_THROUGHPUT, bytesWritten, "topic=" + topic,
        "host=" + HOSTNAME, "logname=" + logName);
    OpenTsdbMetricConverter.gauge(SingerMetrics.KAFKA_LATENCY, maxKafkaBatchWriteLatency,
        "topic=" + topic, "host=" + HOSTNAME, "logname=" + logName);
    OpenTsdbMetricConverter.incr(SingerMetrics.NUM_KAFKA_MESSAGES, numLogMessages,
        "topic=" + topic, "host=" + HOSTNAME, "logname=" + logName);
    OpenTsdbMetricConverter.incr(SingerMetrics.SINGER_WRITER 
        + "num_committable_kafka_messages_delivery_success", numLogMessages,
        "topic=" + topic, "host=" + HOSTNAME, "logname=" + logName);
  }

  @Override
  public boolean isCommittableWriter() {
    return true;
  }

  protected final class KafkaWriteTask implements Callable<KafkaWritingTaskResult> {
    private final Entry<Integer, KafkaWritingTaskFuture> entry;

    protected KafkaWriteTask(Entry<Integer, KafkaWritingTaskFuture> entry) {
      this.entry = entry;
    }

    @Override
    public KafkaWritingTaskResult call() throws LogStreamWriterException {
      KafkaWritingTaskResult result = null;
      KafkaWritingTaskFuture task = entry.getValue();
      int size = task.getRecordMetadataList().size();
      PartitionInfo partitionInfo = task.getPartitionInfo();
      int leaderNode = partitionInfo.leader().id();
      if (size > 0) {
        OpenTsdbMetricConverter.addMetric(SingerMetrics.WRITER_BATCH_SIZE, size, "topic=" + topic,
            "host=" + KafkaWriter.HOSTNAME);
      }
      try {
        List<RecordMetadata> recordMetadataList = new ArrayList<>();
        int bytesWritten = 0;
        for (Future<RecordMetadata> future : task.getRecordMetadataList()) {
          if (future.isCancelled()) {
            result = new KafkaWritingTaskResult(false, 0, 0);
            break;
          } else {
            // We will get TimeoutException if the wait timed out
            RecordMetadata recordMetadata = future.get(writeTimeoutInSeconds, TimeUnit.SECONDS);

            // used for tracking metrics
            if (recordMetadata != null) {
              bytesWritten += recordMetadata.serializedKeySize()
                  + recordMetadata.serializedValueSize();
              recordMetadataList.add(recordMetadata);
            }
          }
        }
        if (result == null) {
          // we can down convert since latency should be less that Integer.MAX_VALUE
          int kafkaLatency = (int) (System.currentTimeMillis() - task.getFirstProduceTimestamp());
          // we shouldn't have latency creater than 2B milliseoncds so it should be okay
          // to downcast to integer
          result = new KafkaWritingTaskResult(true, bytesWritten, (int) kafkaLatency);
          result.setRecordMetadataList(recordMetadataList);
          result.setPartition(partitionInfo.partition());
          OpenTsdbMetricConverter.incrGranular(SingerMetrics.BROKER_WRITE_SUCCESS, 1,
              "broker=" + leaderNode);
          OpenTsdbMetricConverter.addGranularMetric(SingerMetrics.BROKER_WRITE_LATENCY,
              kafkaLatency, "broker=" + leaderNode);
        }
      } catch (org.apache.kafka.common.errors.RecordTooLargeException e) {
        LOG.error("Kafka write failure due to excessively large message size", e);
        OpenTsdbMetricConverter.incr(SingerMetrics.OVERSIZED_MESSAGES, 1, "topic=" + topic,
            "host=" + KafkaWriter.HOSTNAME);
        result = new KafkaWritingTaskResult(false, e);
      } catch (org.apache.kafka.common.errors.SslAuthenticationException e) {
        LOG.error("Kafka write failure due to SSL authentication failure", e);
        OpenTsdbMetricConverter.incr(SingerMetrics.WRITER_SSL_EXCEPTION, 1, "topic=" + topic,
            "host=" + KafkaWriter.HOSTNAME);
        result = new KafkaWritingTaskResult(false, e);
      } catch (Exception e) {
        String errorMsg = "Failed to write " + size + " messages to kafka";
        LOG.error(errorMsg, e);
        OpenTsdbMetricConverter.incr(SingerMetrics.WRITE_FAILURE, 1, "topic=" + topic,
            "host=" + KafkaWriter.HOSTNAME);
        OpenTsdbMetricConverter.incrGranular(SingerMetrics.BROKER_WRITE_FAILURE, 1,
            "broker=" + leaderNode);
        result = new KafkaWritingTaskResult(false, e);
      } finally {
        if (result != null && !result.success) {
          for (Future<RecordMetadata> future : task.getRecordMetadataList()) {
            if (!future.isCancelled() && !future.isDone()) {
              future.cancel(true);
            }
          }
        }
      }
      return result;
    }
  }

  @VisibleForTesting
  protected Map<Integer, KafkaWritingTaskFuture> getCommitableBuckets() {
    return commitableBuckets;
  }

}
