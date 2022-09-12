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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

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

/**
 * Committable writer that implements the commit design pattern methods of {@link LogStreamWriter}
 * 
 * This class allows usage of MemoryEfficientLogStreamProcessor.
 */
public class CommittableKafkaWriter extends KafkaWriter {

  private static final Logger LOG = LoggerFactory.getLogger(CommittableKafkaWriter.class);
  public static final String MESSAGE_ID = "_mid";
  public static final String ORIGINAL_TIMESTAMP = "_ots";
  protected List<PartitionInfo> committableValidPartitions;
  protected Map<Integer, Map<Integer, LoggingAuditHeaders>> committableMapOfTrackedMessageMaps;
  protected Map<Integer, Map<Integer, LoggingAuditHeaders>> committableMapOfInvalidMessageMaps;
  protected Map<Integer, Integer> committableMapOfOriginalIndexWithinBucket;
  protected Map<Integer, KafkaWritingTaskFuture> committableBuckets;
  protected KafkaProducer<byte[], byte[]> committableProducer;
  protected static final ScheduledExecutorService executionTimer;
  static {
    ScheduledThreadPoolExecutor tmpTimer = new ScheduledThreadPoolExecutor(1);
    tmpTimer.setRemoveOnCancelPolicy(true);
    executionTimer = tmpTimer;
  }


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
  public void startCommit(boolean isDraining) throws LogStreamWriterException {
    committableProducer = KafkaProducerManager.getProducer(producerConfig);
    Preconditions.checkNotNull(committableProducer);
    List<PartitionInfo> partitions;
    try {
      partitions = committableProducer.partitionsFor(topic);
    } catch (Exception e) {
      LOG.error("Exception when calling partitionsFor on topic " + topic + ", resetting producer", e);
      KafkaProducerManager.resetProducer(producerConfig);
      OpenTsdbMetricConverter.incr("singer.writer.start_commit.error", 1, "topic=" + topic,
          "host=" + HOSTNAME, "drain=" + isDraining);
      OpenTsdbMetricConverter.incr("singer.writer.producer_reset", 1, "topic=" + topic,
          "host=" + HOSTNAME, "drain=" + isDraining);
      throw e;
    }

    if (producerConfig.isTransactionEnabled()) {
      committableProducer.beginTransaction();
    }

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

    committableBuckets = new HashMap<>();
    committableMapOfTrackedMessageMaps = new HashMap<>();
    committableMapOfInvalidMessageMaps = new HashMap<>();
    committableMapOfOriginalIndexWithinBucket = new HashMap<>();

    for (PartitionInfo partitionInfo : committableValidPartitions) {
      // for each partitionId, there is a corresponding bucket in buckets and a
      // corresponding headersMap in mapOfHeadersMaps.
      int partitionId = partitionInfo.partition();
      committableBuckets.put(partitionId, new KafkaWritingTaskFuture(partitionInfo));
      committableMapOfTrackedMessageMaps.put(partitionId, new HashMap<>());
      committableMapOfInvalidMessageMaps.put(partitionId, new HashMap<>());
      committableMapOfOriginalIndexWithinBucket.put(partitionId, -1);
    }
  }

  @Override
  public void writeLogMessageToCommit(LogMessageAndPosition message, boolean isDraining) throws LogStreamWriterException {
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
    addStandardHeaders(message, headers);
    checkAndSetLoggingAuditHeadersForLogMessage(msg);
    committableMapOfOriginalIndexWithinBucket.put(partitionId, 1 + committableMapOfOriginalIndexWithinBucket.get(partitionId));
    if (msg.getLoggingAuditHeaders() != null) {
      // check if the message should be skipped
      if (checkMessageValidAndInjectHeaders(msg, headers, committableMapOfOriginalIndexWithinBucket.get(partitionId), partitionId,
          committableMapOfTrackedMessageMaps, committableMapOfInvalidMessageMaps)) {
        return;
      }
    }

    KafkaWritingTaskFuture kafkaWritingTaskFutureResult = committableBuckets.get(partitionId);
    List<CompletableFuture<RecordMetadata>> recordMetadataList = kafkaWritingTaskFutureResult
        .getRecordMetadataList();

    if (recordMetadataList.isEmpty()) {
      kafkaWritingTaskFutureResult.setFirstProduceTimestamp(System.currentTimeMillis());
    }

    CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
    committableProducer.send(keyedMessage, (recordMetadata, exception) -> {
      if (exception != null) {
        future.completeExceptionally(exception);
      } else {
        future.complete(recordMetadata);
      }
    });
    recordMetadataList.add(future);
  }

  public void addStandardHeaders(LogMessageAndPosition message, Headers headers) {
    headers.add(MESSAGE_ID,
        ByteBuffer.wrap(new byte[SINGER_DEFAULT_MESSAGEID_LENGTH])
            .putLong(message.getNextPosition().getLogFile().getInode())
            .putLong(message.getNextPosition().getByteOffset()).array());
    headers.add(ORIGINAL_TIMESTAMP,
        ByteBuffer.allocate(8).putLong(message.getLogMessage().getTimestampInNanos()).array());
    if (message.isSetInjectedHeaders()) {
      Map<String, ByteBuffer> injectedHeaders = message.getInjectedHeaders();
      for (Entry<String, ByteBuffer> entry : injectedHeaders.entrySet()) {
        headers.add(entry.getKey(), entry.getValue().array());
      }
    }
  }

  @Override
  public void endCommit(int numLogMessages, boolean isDraining) throws LogStreamWriterException {

    List<CompletableFuture<Integer>> bucketFutures = new ArrayList<>();
    for(KafkaWritingTaskFuture f : committableBuckets.values()) {
      List<CompletableFuture<RecordMetadata>> futureList = f.getRecordMetadataList();
      if (futureList.isEmpty()) {
        continue;
      }
      long start = f.getFirstProduceTimestamp();
      int leaderNode = f.getPartitionInfo().leader().id();
      int size = futureList.size();
      OpenTsdbMetricConverter.addMetric(SingerMetrics.WRITER_BATCH_SIZE, size, "topic=" + topic,
          "host=" + KafkaWriter.HOSTNAME);

      // resolves with the latency of that bucket
      CompletableFuture<Integer> bucketFuture = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
          .handleAsync((v, t) -> {
            if (t != null) {
              handleBucketException(leaderNode, size, isDraining, t);
              if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
              } else {
                throw new RuntimeException(t);
              }
            }
            int kafkaLatency = (int) (System.currentTimeMillis() - start);
            // we shouldn't have latency greater than 2B milliseconds so it should be okay
            // to downcast to integer
            OpenTsdbMetricConverter.incrGranular(SingerMetrics.BROKER_WRITE_SUCCESS, 1,
                "broker=" + leaderNode, "drain=" + isDraining);
            OpenTsdbMetricConverter.addGranularMetric(SingerMetrics.BROKER_WRITE_LATENCY,
                kafkaLatency, "broker=" + leaderNode, "drain=" + isDraining);
            return kafkaLatency;
          });
      bucketFutures.add(bucketFuture);
    }
    CompletableFuture<Void> batchFuture = CompletableFuture.allOf(bucketFutures.toArray(new CompletableFuture[0]));

    // Set a timeout task that will cause the batch future to fail after writeTimeoutInSeconds
    CompletableFuture<Void> timerFuture = new CompletableFuture<>();
    Future<?> timerTask = executionTimer.schedule(() -> {
      timerFuture.completeExceptionally(new TimeoutException("Kafka batch write timed out after " + writeTimeoutInSeconds + " seconds"));
    }, writeTimeoutInSeconds, TimeUnit.SECONDS);

    CompletableFuture<Void> writerFuture = batchFuture
        .applyToEitherAsync(timerFuture, Function.identity())
        .whenComplete(
            (v, t) -> {
              if (t != null) {
                handleBatchException(numLogMessages, isDraining, t);
              } else {
                timerTask.cancel(true);
                onBatchComplete(numLogMessages, bucketFutures, isDraining);
              }
            }
        );
    try {
      writerFuture.get();
    } catch (CompletionException | InterruptedException | ExecutionException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new LogStreamWriterException("Failed to write messages to topic " + topic, e);
    }
  }

  protected void handleBucketException(int leaderNode, int size, boolean isDraining, Throwable t) {
    if (t instanceof org.apache.kafka.common.errors.RecordTooLargeException) {
      LOG.error("Kafka write failure due to excessively large message size", t);
      OpenTsdbMetricConverter.incr(SingerMetrics.OVERSIZED_MESSAGES, 1, "topic=" + topic,
          "host=" + KafkaWriter.HOSTNAME, "drain=" + isDraining);
    } else if (t instanceof org.apache.kafka.common.errors.SslAuthenticationException) {
      LOG.error("Kafka write failure due to SSL authentication failure", t);
      OpenTsdbMetricConverter.incr(SingerMetrics.WRITER_SSL_EXCEPTION, 1, "topic=" + topic,
          "host=" + KafkaWriter.HOSTNAME, "drain=" + isDraining);
    } else if (t instanceof Exception) {
      LOG.error("Failed to write " + size + " messages to kafka", t);
      OpenTsdbMetricConverter.incr(SingerMetrics.WRITE_FAILURE, 1, "topic=" + topic,
          "host=" + KafkaWriter.HOSTNAME, "drain=" + isDraining);
      OpenTsdbMetricConverter.incrGranular(SingerMetrics.BROKER_WRITE_FAILURE, 1,
          "broker=" + leaderNode, "drain=" + isDraining);
    }
  }

  protected void onBatchComplete(int numLogMessages, List<CompletableFuture<Integer>> bucketFutures, boolean isDraining) {
    int bytesWritten = 0;
    for (Entry<Integer, KafkaWritingTaskFuture> entry : committableBuckets.entrySet()) {
      List<CompletableFuture<RecordMetadata>> futureList = entry.getValue().getRecordMetadataList();
      if (futureList.isEmpty()) {
        continue;
      }
      List<RecordMetadata> recordMetadataList = futureList.stream()
          .map(CompletableFuture::join)
          .collect(Collectors.toList());
      if (isLoggingAuditEnabledAndConfigured()) {
        captureAndLogAuditEvents(entry.getKey(), recordMetadataList);
      }
      bytesWritten += recordMetadataList.stream().mapToInt(rmd -> rmd.serializedKeySize() + rmd.serializedValueSize()).sum();
    }
    int maxKafkaBatchWriteLatency = bucketFutures.stream().mapToInt(CompletableFuture::join).max().orElse(0);
    if (producerConfig.isTransactionEnabled()) {
      committableProducer.commitTransaction();
      OpenTsdbMetricConverter.incr(SingerMetrics.NUM_COMMITED_TRANSACTIONS, 1, "topic=" + topic,
          "host=" + HOSTNAME, "logname=" + logName);
    }
    updateWriteSuccessMetrics(numLogMessages, bytesWritten, maxKafkaBatchWriteLatency, isDraining);
  }

  protected void handleBatchException(int numLogMessages, boolean isDraining, Throwable t) {
    LOG.error("Caught exception when write " + numLogMessages + " messages to producer.", t);

    SingerRestartConfig restartConfig = SingerSettings.getSingerConfig().singerRestartConfig;
    if (restartConfig != null && restartConfig.restartOnFailures
        && failureCounter.incrementAndGet() > restartConfig.numOfFailuesAllowed) {
      LOG.error("Encountered {} kafka logging failures.", failureCounter.get());
    }
    if (producerConfig.isTransactionEnabled()) {
      committableProducer.abortTransaction();
      OpenTsdbMetricConverter.incr(SingerMetrics.NUM_ABORTED_TRANSACTIONS, 1, "topic=" + topic,
          "host=" + HOSTNAME, "logname=" + logName, "drain=" + isDraining);
    }
    KafkaProducerManager.resetProducer(producerConfig);
    updateWriteFailureMetrics(numLogMessages, isDraining);
    throw new CompletionException("Failed to write messages to topic " + topic, t);
  }

  private void captureAndLogAuditEvents(int bucketIndex, List<RecordMetadata> recordMetadataList) {
    if (isLoggingAuditEnabledAndConfigured()) {
      enqueueLoggingAuditEvents(recordMetadataList, committableMapOfTrackedMessageMaps.get(bucketIndex),
          committableMapOfInvalidMessageMaps.get(bucketIndex));
    }
  }

  private void updateWriteFailureMetrics(int numLogMessages, boolean isDraining) {
    OpenTsdbMetricConverter.incr("singer.writer.producer_reset", 1, "topic=" + topic,
        "host=" + HOSTNAME, "drain=" + isDraining);
    OpenTsdbMetricConverter.incr("singer.writer.num_kafka_messages_delivery_failure",
        numLogMessages, "topic=" + topic, "host=" + HOSTNAME, "logname=" + logName, "drain=" + isDraining);
    OpenTsdbMetricConverter.incr(SingerMetrics.SINGER_WRITER
            + "num_committable_kafka_messages_delivery_failure", numLogMessages,
        "topic=" + topic, "host=" + HOSTNAME, "logname=" + logName, "drain=" + isDraining);
  }

  private void updateWriteSuccessMetrics(int numLogMessages, int bytesWritten, int maxKafkaBatchWriteLatency, boolean isDraining) {
    OpenTsdbMetricConverter.gauge(SingerMetrics.KAFKA_THROUGHPUT, bytesWritten, "topic=" + topic,
        "host=" + HOSTNAME, "logname=" + logName, "drain=" + isDraining);
    OpenTsdbMetricConverter.gauge(SingerMetrics.KAFKA_LATENCY, maxKafkaBatchWriteLatency,
        "topic=" + topic, "host=" + HOSTNAME, "logname=" + logName, "drain=" + isDraining);
    OpenTsdbMetricConverter.incr(SingerMetrics.NUM_KAFKA_MESSAGES, numLogMessages,
        "topic=" + topic, "host=" + HOSTNAME, "logname=" + logName, "drain=" + isDraining);
    OpenTsdbMetricConverter.incr(SingerMetrics.SINGER_WRITER
            + "num_committable_kafka_messages_delivery_success", numLogMessages,
        "topic=" + topic, "host=" + HOSTNAME, "logname=" + logName, "drain=" + isDraining);
  }

  @Override
  public boolean isCommittableWriter() {
    return true;
  }

  @VisibleForTesting
  protected Map<Integer, KafkaWritingTaskFuture> getCommittableBuckets() {
    return committableBuckets;
  }

}
