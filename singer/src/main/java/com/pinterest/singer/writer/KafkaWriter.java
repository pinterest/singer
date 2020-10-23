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

import com.google.common.primitives.Longs;
import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.LogStreamWriter;
import com.pinterest.singer.common.errors.LogStreamWriterException;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.loggingaudit.client.AuditHeadersGenerator;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditHeaders;
import com.pinterest.singer.loggingaudit.thrift.configuration.AuditConfig;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.thrift.configuration.SingerRestartConfig;
import com.pinterest.singer.utils.CommonUtils;
import com.pinterest.singer.utils.PartitionComparator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.header.Headers;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

/**
 * LogStreamWriter implementation that write to Kafka cluster.
 * <p/>
 * This class is NOT thread-safe.
 */
public class KafkaWriter implements LogStreamWriter {

  public static final String HOSTNAME = SingerSettings.getEnvironment().getHostname();
  private static final Logger LOG = LoggerFactory.getLogger(KafkaWriter.class);
  protected static final PartitionComparator COMPARATOR = new PartitionComparator();

  private static final ThreadLocal<TSerializer> SERIALIZER = ThreadLocal.withInitial(TSerializer::new);
  private static ThreadLocal<CRC32> localCRC = ThreadLocal.withInitial(CRC32::new);
  private static final String LOGGING_AUDIT_HEADER_KEY = "loggingAuditHeaders";
  private static final String CRC_HEADER_KEY = "messageCRC";


  // Counter for the number of batch message writing failures
  protected static AtomicInteger failureCounter = new AtomicInteger(0);

  protected final LogStream logStream;

  // Topic to which this LogWriter writes.
  protected final String topic;

  protected final String logName;

  protected final boolean skipNoLeaderPartitions;

  protected final boolean auditingEnabled;

  protected final KafkaProducerConfig producerConfig;

  protected final String kafkaClusterSig;

  protected final ExecutorService clusterThreadPool;

  protected final int writeTimeoutInSeconds;

  protected KafkaMessagePartitioner partitioner;

  protected boolean enableHeadersInjector = false;

  protected boolean enableLoggingAudit = false;

  protected AuditHeadersGenerator auditHeadersGenerator = null;

  protected AuditConfig auditConfig = null;

  /**
   *  HeadersInjector will be set if enableHeadersInjector is set to true.
   *  Default is null
  */
  protected HeadersInjector headersInjector = null;


  public boolean isEnableLoggingAudit() {
    return enableLoggingAudit;
  }

  public AuditHeadersGenerator getAuditHeadersGenerator() {
    return auditHeadersGenerator;
  }

  @VisibleForTesting
  public void setAuditHeadersGenerator(
      AuditHeadersGenerator auditHeadersGenerator) {
    this.auditHeadersGenerator = auditHeadersGenerator;
  }

  public AuditConfig getAuditConfig() {
    return auditConfig;
  }


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
    if (logStream.getSingerLog().getSingerLogConfig().isEnableLoggingAudit()){
      this.enableLoggingAudit = true;
      this.auditConfig = logStream.getSingerLog().getSingerLogConfig().getAuditConfig();
      this.auditHeadersGenerator = new AuditHeadersGenerator(CommonUtils.getHostName(), logName);
    }
  }

  public KafkaWriter(LogStream logStream,
                     KafkaProducerConfig producerConfig,
                     String topic,
                     boolean skipNoLeaderPartitions,
                     boolean auditingEnabled,
                     String auditTopic,
                     String partitionerClassName,
                     int writeTimeoutInSeconds, boolean enableHeadersInjector) throws Exception {
    this(logStream, producerConfig, topic, skipNoLeaderPartitions, auditingEnabled, auditTopic,
        partitionerClassName, writeTimeoutInSeconds);
    this.enableHeadersInjector = enableHeadersInjector;
    if (this.enableHeadersInjector) {
      try{
        String headersInjectorClass = logStream.getSingerLog().getSingerLogConfig().getHeadersInjectorClass();
        @SuppressWarnings("unchecked")
        Class<HeadersInjector> cls = (Class<HeadersInjector>)Class.forName(headersInjectorClass);
        headersInjector = cls.newInstance();
        LOG.warn("HeadersInjector has been configured to: " + headersInjector.getClass().getName());
      } catch (Exception e){
        LOG.error("failed to load and set SingerLogConfig's headersInjector");
      }
    }
  }

  @VisibleForTesting
  protected KafkaWriter(LogStream logStream,
                        KafkaProducerConfig producerConfig,
                        KafkaMessagePartitioner partitioner,
                        String topic,
                        boolean skipNoLeaderPartitions,
                        ExecutorService clusterThreadPool,
                        boolean enableHeadersInjector) {
    this.logStream = logStream;
    this.partitioner = partitioner;
    this.skipNoLeaderPartitions = skipNoLeaderPartitions;
    this.producerConfig = producerConfig;
    this.clusterThreadPool = clusterThreadPool;
    this.topic = topic;
    this.enableHeadersInjector = enableHeadersInjector;
    logName = logStream.getSingerLog().getSingerLogConfig().getName();
    writeTimeoutInSeconds = 0;
    auditingEnabled = false;
    kafkaClusterSig = null;
    if (this.enableHeadersInjector){
      try{
        String headersInjectorClass = logStream.getSingerLog().getSingerLogConfig().getHeadersInjectorClass();
        @SuppressWarnings("unchecked")
        Class<HeadersInjector> cls = (Class<HeadersInjector>)Class.forName(headersInjectorClass);
        headersInjector = cls.newInstance();
        LOG.warn("HeadersInjector has been configured to: " + headersInjector.getClass().getName());
      } catch (Exception e){
        LOG.error("failed to load and set SingerLogConfig's headersInjector");
      }
    }
    if (logStream != null && logStream.getSingerLog().getSingerLogConfig().isEnableLoggingAudit()){
      this.enableLoggingAudit = true;
      this.auditConfig = logStream.getSingerLog().getSingerLogConfig().getAuditConfig();
      this.auditHeadersGenerator = new AuditHeadersGenerator(CommonUtils.getHostName(), logName);
    }
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
    auditingEnabled = false;
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

  @VisibleForTesting
  public HeadersInjector getHeadersInjector() {
    return headersInjector;
  }

  @VisibleForTesting
  public ExecutorService getClusterThreadPool() {
    return clusterThreadPool;
  }

  /**
   * Distribute the message batch into different partitions, and use the under-lying kafka writing
   * thread pool to speed up writing.
   *
   * If skipNoLeaderPartitions flag is set, we will skip the partitions that have no leader.
   *
   * @param partitions unordered list of partitionInfo to be used for partitioning this batch
   * @param topic the kafka topic
   * @param logMessages the messages that will be written to kafka
   * @param mapOfTrackedMessageMaps a map whose key is the partitionId, value is an inner map. The
   *                               key of the inner map is indexWithinTheBucket and the value of the
   *                               inner map is LoggingAuditHeaders object of tracked messages
   * @param mapOfInvalidMessageMaps a map whose key is the partitionId, value is an inner map. The
   *                               key of the inner map is indexWithinTheBucket and the value of the
   *                               inner map is LoggingAuditHeaders object of invalid messages(note:
   *                               invalid messages may or may not be tracked)
   * @param mapOfOriginalIndexWithinBucket a map whose key is the partitionId, value is original
   *                                       index within the bucket. The init value  is -1. Anytime
   *                                       a message is assigned to a partition (may be skipped),
   *                                       the value increases by 1. If certain messages are skipped
   *                                       due to corruption, i.e. they are not added to the bucket,
   *                                       the actual index of those un-skipped messages could be
   *                                       lower. The original index means assuming no messages are
   *                                       skipped.
   *                                       For example: for ith partition, there are 6 message m0,
   *                                       m1, m2, m3, m4, m5 assigned. The original index will be
   *                                       0, 1, 2, 3, 4, 5. If m1 and m3 are skipped due to
   *                                       corruption, the actual messages sent to Kafka will be
   *                                       m0, m2, m4, m5 and actual index will be 0, 1, 2, 3.
   *
   * @return a list of message lists that are classified based on partitions.
   */
  Map<Integer, List<ProducerRecord<byte[], byte[]>>> messageCollation(
      List<PartitionInfo> partitions,
      String topic,
      List<LogMessage> logMessages,
      Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfTrackedMessageMaps,
      Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfInvalidMessageMaps,
      Map<Integer, Integer> mapOfOriginalIndexWithinBucket) throws Exception {
    LOG.info("Collate {} messages of topic {} for logStream {}", logMessages.size(), topic, logName);

    Map<Integer, List<ProducerRecord<byte[], byte[]>>> buckets = new HashMap<>();
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

      for (int i = 0; i < validPartitions.size(); i++) {
        // for each partitionId, there is a corresponding bucket in buckets and a corresponding
        // trackedMessageMap in mapOfTrackedMessageMaps and corresponding invalidMessageMap in
        // mapOfInvalidMessageMaps
        int partitionId = validPartitions.get(i).partition();
        buckets.put(partitionId, new ArrayList<>());
        mapOfTrackedMessageMaps.put(partitionId, new HashMap<Integer, LoggingAuditHeaders>());
        mapOfInvalidMessageMaps.put(partitionId, new HashMap<Integer, LoggingAuditHeaders>());
        mapOfOriginalIndexWithinBucket.put(partitionId, -1);
      }

      ProducerRecord<byte[], byte[]> keyedMessage;
      for (LogMessage msg : logMessages) {
        byte[] key = null;
        if (msg.isSetKey()) {
          key = msg.getKey();
        }
        int partitionId = partitioner.partition(key, validPartitions);
        if (skipNoLeaderPartitions) {
          partitionId = validPartitions.get(partitionId).partition();
        }
        keyedMessage = new ProducerRecord<>(topic, partitionId, key, msg.getMessage());
        mapOfOriginalIndexWithinBucket.put(partitionId, 1 + mapOfOriginalIndexWithinBucket.get(partitionId));
        Headers headers = keyedMessage.headers();
        checkAndSetLoggingAuditHeadersForLogMessage(msg);
        if (msg.getLoggingAuditHeaders() != null) {
          // check if the message should be skipped
          if (checkMessageValidAndInjectHeaders(msg, headers, mapOfOriginalIndexWithinBucket.get(partitionId),
              partitionId, mapOfTrackedMessageMaps, mapOfInvalidMessageMaps)) {
            continue;
          }
        }
        buckets.get(partitionId).add(keyedMessage);
      }
    } catch (Exception e) {
      LOG.error("Failed in message collation for topic {}, partitioner {}", topic, partitioner.getClass().getName(), e);
      throw new LogStreamWriterException(e.toString());
    }
    return buckets;
  }

  /**
   *  Validate the message and inject headers for the ProducerRecord
   *
   *  If the message is corrupted and corrupted messages are configured to be skipped at the current
   *  stage, return true. Otherwise, return false, since the message should not be skipped
   *
   * @param msg the message to validate
   * @param headers the headers of the ProducerRecord
   * @param indexWithinTheBucket the index of the current message within the bucket
   * @param partitionId the partition id
   * @param mapOfTrackedMessageMaps a map whose key is the partitionId, value is an inner map. The
   *                               key of the inner map is indexWithinTheBucket and the value of the
   *                               inner map is LoggingAuditHeaders object of tracked messages
   * @param mapOfInvalidMessageMaps a map whose key is the partitionId, value is an inner map. The
   *                               key of the inner map is indexWithinTheBucket and the value of the
   *                               inner map is LoggingAuditHeaders object of invalid messages(note:
   *                               invalid messages may or may not be tracked)
   * @return a boolean indicates whether this message should be skipped
   */
  public boolean checkMessageValidAndInjectHeaders(
      LogMessage msg, Headers headers, int indexWithinTheBucket, int partitionId,
      Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfTrackedMessageMaps,
      Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfInvalidMessageMaps) {
    boolean isMessageValid = checkMessageValid(msg);

    // note that only a percentage of messages are tracked, i.e. corresponding audit events are sent
    // out at different stages. For each bucket, messages being tracked are kept in the hash map where
    // key is the indexWithin the bucket, and value is the message LoggingAuditHeaders.
    if (msg.getLoggingAuditHeaders() != null && msg.getLoggingAuditHeaders().isTracked()){
      mapOfTrackedMessageMaps.get(partitionId).put(indexWithinTheBucket, msg.getLoggingAuditHeaders());
    }

    // note that some messages (whether being tracked or not) could be invalid because crc32 checksum
    // does not match mismatch or original message cannot be deserialized. These invalid messages are
    // kept in the hash map where key is the indexWithin the bucket, and value is the message
    // LoggingAuditHeaders.
    if (msg.getLoggingAuditHeaders() != null && !isMessageValid) {
      mapOfInvalidMessageMaps.get(partitionId).put(indexWithinTheBucket, msg.getLoggingAuditHeaders());
    }

    boolean shouldSkipMessage = false;
    if (enableLoggingAudit && auditConfig.isSkipCorruptedMessageAtCurrentStage() && !isMessageValid) {
      OpenTsdbMetricConverter.incr(SingerMetrics.AUDIT_NUM_INVALID_MESSAGES_SKIPPED, "topic=" + topic,
          "host=" + HOSTNAME, "logName=" + msg.getLoggingAuditHeaders().getLogName(),
          "logStreamName=" + logName);
      shouldSkipMessage = true;
    }
    if (this.headersInjector != null) {
      injectHeadersForProducerRecord(msg, headers);
    }
    return shouldSkipMessage;
  }

  protected void injectHeadersForProducerRecord(LogMessage msg, Headers headers) {
    try {
      if (msg.isSetLoggingAuditHeaders()) {
        byte[] serializedAuditHeaders = SERIALIZER.get().serialize(msg.getLoggingAuditHeaders());
        this.headersInjector.addHeaders(headers, LOGGING_AUDIT_HEADER_KEY, serializedAuditHeaders);
        OpenTsdbMetricConverter.incr(SingerMetrics.AUDIT_HEADERS_INJECTED,  "host=" + HOSTNAME, "logStreamName=" + logName);
      }
      if (msg.isSetChecksum()) {
        this.headersInjector.addHeaders(headers, CRC_HEADER_KEY, Longs.toByteArray(msg.getChecksum()));
        OpenTsdbMetricConverter.incr(SingerMetrics.CHECKSUM_INJECTED,  "host=" + HOSTNAME, "logStreamName=" + logName);
      }
    } catch (TException e) {
      OpenTsdbMetricConverter.incr(SingerMetrics.NUMBER_OF_SERIALIZING_HEADERS_ERRORS);
      LOG.warn("Exception thrown while serializing headers", e);
    }
  }

  protected boolean checkMessageValid(LogMessage msg) {
    if (msg.getMessage() == null){
      return false;
    }
    boolean isMessageUncorrupted = true;
    boolean canDeserializeMessage = true;

    // check if message is corrupted based on crc32 checksum
    if (msg.isSetChecksum()) {
      long start = System.nanoTime();
      long singerChecksum = computeCRC(msg.getMessage());
      isMessageUncorrupted = singerChecksum == msg.getChecksum();
      OpenTsdbMetricConverter.gauge(SingerMetrics.AUDIT_COMPUTE_CHECKSUM_LATENCY_NANO, Math.max(
          0, System.nanoTime() - start), "host=" + HOSTNAME, "logStreamName=" + logName);
      OpenTsdbMetricConverter.incr(isMessageUncorrupted ? SingerMetrics.AUDIT_NUM_UNCORRUPTED_MESSAGES : SingerMetrics.AUDIT_NUM_CORRUPTED_MESSAGES,
          "host=" + HOSTNAME, "logStreamName=" + logName);
    }
    //TODO check if message can be deserialized.

    return isMessageUncorrupted && canDeserializeMessage;
  }

  private long computeCRC(byte[] message) {
    CRC32 crc = localCRC.get();
    crc.reset();
    crc.update(message);
    return crc.getValue();
  }

  /**
   * If auditing is started at Singer, LoggingAuditHeaders and crc32 checksum are injected for
   * every message. Based on audit rate, certain messages are randomly chosen to be tracked.
   * Tracked messages will have audit event sent out at Singer and later stages.
   * @param msg
   */
  public void checkAndSetLoggingAuditHeadersForLogMessage(LogMessage msg){
    if (enableLoggingAudit && auditConfig.isStartAtCurrentStage()){
      LoggingAuditHeaders loggingAuditHeaders = null;
      try {
        loggingAuditHeaders = auditHeadersGenerator.generateHeaders();
        if (ThreadLocalRandom.current().nextDouble() < auditConfig.getSamplingRate()) {
          loggingAuditHeaders.setTracked(true);
        }
        long checksum = computeCRC(msg.getMessage());
        msg.setLoggingAuditHeaders(loggingAuditHeaders);
        msg.setChecksum(checksum);
        LOG.debug("Setting loggingAuditHeaders {} for {}", loggingAuditHeaders, logName);
        OpenTsdbMetricConverter.incr(SingerMetrics.AUDIT_HEADERS_SET_FOR_LOG_MESSAGE,
            "topic=" + topic, "host=" + HOSTNAME,  "logName=" +
                msg.getLoggingAuditHeaders().getLogName(), "logStreamName=" + logName);
        if (loggingAuditHeaders.isTracked()) {
          OpenTsdbMetricConverter.incr(SingerMetrics.AUDIT_HEADERS_TRACKED_FOR_LOG_MESSAGE,
              "topic=" + topic, "host=" + HOSTNAME, "logName=" +
                  msg.getLoggingAuditHeaders().getLogName(), "logStreamName=" + logName);
        }
      } catch (Exception e){
        OpenTsdbMetricConverter.incr(SingerMetrics.AUDIT_HEADERS_SET_FOR_LOG_MESSAGE_EXCEPTION,
            "topic=" + topic, "host=" + HOSTNAME,  "logName=" +
                msg.getLoggingAuditHeaders().getLogName(), "logStreamName=" + logName);
        LOG.debug("Couldn't set loggingAuditHeaders {} for {} as logging audit is enabled "
            + "and start at Singer {} ", loggingAuditHeaders, logName,  auditConfig);
      }
    }
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

      // The key of mapOfTrackedMessageMaps is the partition_id; value of mapOfTrackedMessageMaps is
      // trackedMessageMap. key of the trackedMessageMap is the listIndex of ProducerRecord in the
      // bucket (buckets.get(partition_id)); value of the trackedMessageMap is the
      // LoggingAuditHeaders of the tracked message.
      Map<Integer, Map<Integer, LoggingAuditHeaders>>  mapOfTrackedMessageMaps = new HashMap<>();

      // The key of mapOfInvalidMessageMaps is the partition_id; value of mapOfInvalidMessageMaps is
      // invalidMessageMap. The key of the invalidMessageMap is the listIndex of ProducerRecord in the
      // bucket (buckets.get(partition_id)); value of the invalidMessageMap is the
      // LoggingAuditHeaders of the invalid message. Message is determined as invalid because crc32
      // checksum does not match or message cannot be deserialized.
      Map<Integer, Map<Integer, LoggingAuditHeaders>>  mapOfInvalidMessageMaps = new HashMap<>();



      // The key of mapOfOriginalIndexWithinBucket is the partition_id, value is original index
      // within the bucket (one bucket corresponds to one partition_id). The init value is -1,
      // anytime a LogMessage is assigned to partition_id, the value increases by 1.

      Map<Integer, Integer> mapOfOriginalIndexWithinBucket = new HashMap<>();

      // key of buckets is the partition_id; value of the buckets is a list of ProducerRecord that
      // should be sent to partition_id.
      Map<Integer, List<ProducerRecord<byte[], byte[]>>> buckets = messageCollation(partitions,
          topic, logMessages, mapOfTrackedMessageMaps, mapOfInvalidMessageMaps, mapOfOriginalIndexWithinBucket);

      // we sort this info after, we have to create a copy of the data since
      // the returned list is immutable
      List<PartitionInfo> sortedPartitions = new ArrayList<>(partitions);
      Collections.sort(sortedPartitions, COMPARATOR);

      for (List<ProducerRecord<byte[], byte[]>> msgs : buckets.values()) {
        if (msgs.size() > 0) {
          Callable<KafkaWritingTaskResult> worker =
              new KafkaWritingTask(producer, msgs, writeTimeoutInSeconds, sortedPartitions);
          Future<KafkaWritingTaskResult> future = clusterThreadPool.submit(worker);
          resultSet.add(future);
        }
      }

      int bytesWritten = 0;
      int maxKafkaBatchWriteLatency = 0;
      boolean anyBucketSendFailed = false;

      for (Future<KafkaWritingTaskResult> f : resultSet) {
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
            int bucketIndex = result.getPartition();
            enqueueLoggingAuditEvents(result, mapOfTrackedMessageMaps.get(bucketIndex), mapOfInvalidMessageMaps.get(bucketIndex));
          }
        }
      }
      if (anyBucketSendFailed){
        throw new LogStreamWriterException("Failed to write messages to kafka");
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

  protected boolean isLoggingAuditEnabledAndConfigured(){
    return this.enableLoggingAudit && SingerSettings.getLoggingAuditClient() != null;
  }

  /**
   * enqueues logging audit events for tracked messages and skipped messages. LoggingAuditHeaders of
   * tracked messages has "tracked" field set to be true. Messages will be skipped under 2
   * conditions: (1) message is invalid because crc32 checksum mismatches or message cannot be
   * deserialized; (2) AuditConfig in the SingerLogConfig has skipCorruptedMessageAtCurrentStage set
   * to be true.
   *
   * @param result the KafkaWritingTaskResult
   * @param trackedMessageMap a map in which key is indexWithinTheBucket and value is
   *                          LoggingAuditHeaders object of tracked message
   * @param invalidMessageMap a map in which key is indexWithinTheBucket and value is
   *                          LoggingAuditHeaders object of invalid message
   */
  public void enqueueLoggingAuditEvents(KafkaWritingTaskResult result,
                                        Map<Integer, LoggingAuditHeaders> trackedMessageMap,
                                        Map<Integer, LoggingAuditHeaders> invalidMessageMap){

    //
    if (!enableLoggingAudit || this.auditConfig == null) {
      return;
    }
    if (this.auditConfig.isSkipCorruptedMessageAtCurrentStage()) {
      // invalid messages are skipped and are not sent to Kafka. Suppose there 10 messages in bucket
      // 2 (corresponding to partition 2), 4 messages (withinBucketIndex: 0, 5, 6, 9) are invalid
      // and skipped, thus only 6 messages (withinBucketIndex: 1, 2, 3, 4, 7, 8) are sent to  Kafka
      // which means the RecordMetadataList of result (KafkaWritingTaskResult) should be of size 6.

      int total = result.getRecordMetadataList().size() + invalidMessageMap.size();
      int skippedSofar = 0;
      for(int i = 0; i < total; i++){
        if (invalidMessageMap.containsKey(i)){
          // if message is invalid and also skipped, an audit event should be sent out.
          skippedSofar += 1;
          SingerSettings.getLoggingAuditClient().audit(this.logName, invalidMessageMap.get(i),
              false, -1, true);
        } else {
          if (trackedMessageMap.containsKey(i)){
            // if the message is tracked, an audit event should be sent out.
            int indexInRecordMetadataList = i - skippedSofar;
            if (indexInRecordMetadataList >= result.getRecordMetadataList().size()){
              continue;
            }
            RecordMetadata metadata = result.getRecordMetadataList().get(indexInRecordMetadataList);
            SingerSettings.getLoggingAuditClient().audit(this.logName, trackedMessageMap.get(i),
                true, metadata.timestamp(), kafkaClusterSig, topic);
          }
        }
      }
    } else {
      // In this case, invalid messages are not skipped and still sent to Kafka. This usually means
      // later stage will skip the invalid message.
      int total = result.getRecordMetadataList().size();
      for(int i =0; i < total; i++){
        if (trackedMessageMap.containsKey(i)) {
          int indexInRecordMetadataList = i;
          if (indexInRecordMetadataList >= result.getRecordMetadataList().size()){
            continue;
          }
          RecordMetadata metadata = result.getRecordMetadataList().get(indexInRecordMetadataList);
          SingerSettings.getLoggingAuditClient().audit(this.logName, trackedMessageMap.get(i),
              !invalidMessageMap.containsKey(i),  metadata.timestamp(), kafkaClusterSig, topic);

        }
      }
    }
  }

  public void close() throws IOException {
    // We should not close the producer here since the producer might be shared by multiple
    // LogStreamWriter.
  }
}
