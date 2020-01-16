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
import com.pinterest.singer.loggingaudit.client.AuditHeadersGenerator;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditHeaders;
import com.pinterest.singer.loggingaudit.thrift.configuration.AuditConfig;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.AuditMessage;
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

  public boolean enableHeadersInjector = false;

  private boolean enableLoggingAudit = false;

  private AuditHeadersGenerator auditHeadersGenerator = null;

  private AuditConfig auditConfig = null;

  /**
   *  HeadersInjector will be set if enableHeadersInjector is set to true.
   *  Default is
  */
  private HeadersInjector headersInjector = null;


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
    serializer = null;
    auditingEnabled = false;
    auditTopic = null;
    kafkaClusterSig = null;
    if (this.enableHeadersInjector){
      try{
        String headersInjectorClass = logStream.getSingerLog().getSingerLogConfig().getHeadersInjectorClass();
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

  @VisibleForTesting
  public HeadersInjector getHeadersInjector() {
    return headersInjector;
  }

  @VisibleForTesting
  public ExecutorService getClusterThreadPool() {
    return clusterThreadPool;
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
  Map<Integer, List<ProducerRecord<byte[], byte[]>>> messageCollation(
      List<PartitionInfo> partitions,
      String topic,
      List<LogMessage> logMessages, Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfHeadersMaps) throws Exception {
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

      ProducerRecord<byte[], byte[]> keyedMessage;
      for (int i = 0; i < validPartitions.size(); i++) {
        // for each partitionId, there is a corresponding bucket in buckets and a corresponding
        // headersMap in mapOfHeadersMaps.
        int partitionId = validPartitions.get(i).partition();
        buckets.put(partitionId, new ArrayList<>());
        mapOfHeadersMaps.put(partitionId, new HashMap<Integer, LoggingAuditHeaders>());
      }

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
        Headers headers = keyedMessage.headers();
        checkAndSetLoggingAuditHeadersForLogMessage(msg);
        if (msg.getLoggingAuditHeaders() != null) {
          if (this.headersInjector != null) {
            this.headersInjector.addHeaders(headers, msg);
            OpenTsdbMetricConverter.incr(SingerMetrics.AUDIT_HEADERS_INJECTED, "topic=" + topic, "host=" + HOSTNAME,  "logName=" + msg.getLoggingAuditHeaders().getLogName(), "logStreamName=" + logName);
          }
          // it is the index of the audited message within its bucket.
          // note that not necessarily all messages within a bucket are being audited, thus which
          // message within the bucket being audited should be keep track of for later sending
          // corresponding LoggingAuditEvents.
          int indexWithinTheBucket = buckets.get(partitionId).size();
          mapOfHeadersMaps.get(partitionId).put(indexWithinTheBucket, msg.getLoggingAuditHeaders());
        }
        buckets.get(partitionId).add(keyedMessage);
      }
    } catch (Exception e) {
      LOG.error("Failed in message collation for topic {}, partitioner {}", topic, partitioner.getClass().getName(), e);
      throw new LogStreamWriterException(e.toString());
    }
    return buckets;
  }

  public void checkAndSetLoggingAuditHeadersForLogMessage(LogMessage msg){
    if (enableLoggingAudit && auditConfig.isStartAtCurrentStage() &&
        ThreadLocalRandom.current().nextDouble() < auditConfig.getSamplingRate()){
      LoggingAuditHeaders loggingAuditHeaders = null;
      try {
        loggingAuditHeaders = auditHeadersGenerator.generateHeaders();
        msg.setLoggingAuditHeaders(loggingAuditHeaders);
        LOG.debug("Setting loggingAuditHeaders {} for {}", loggingAuditHeaders, logName);
        OpenTsdbMetricConverter.incr(SingerMetrics.AUDIT_HEADERS_SET_FOR_LOG_MESSAGE,
            "topic=" + topic, "host=" + HOSTNAME,  "logName=" +
                msg.getLoggingAuditHeaders().getLogName(), "logStreamName=" + logName);
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

      // key of mapOfHeadersMap is the partition_id; value of mapOfHeadersMap is HeadersMap.
      // key of the HeadersMap is the the listIndex of ProducerRecord in the
      // bucket (buckets.get(partition_id) ); value of the HashMap is the
      // LoggingAuditHeaders found for this ProducerRecord.
      Map<Integer, Map<Integer, LoggingAuditHeaders>>  mapOfHeadersMap = new HashMap<>();

      // key of buckets is the partition_id; value of the buckets is a list of ProducerRecord that
      // should be sent to partition_id.
      Map<Integer, List<ProducerRecord<byte[], byte[]>>>
          buckets = messageCollation(partitions, topic, logMessages, mapOfHeadersMap);
      
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
            // when result.success is true, the number of recordMetadata SHOULD be the same as
            // the number of ProducerRecord. The size mismatch should never happen.
            // Adding this if-check is just an additional verification to make sure the size match
            // and the audit events sent out is indeed corresponding to those log messages that
            // are audited.
            if (bucketIndex >= 0 && result.getRecordMetadataList().size() != buckets.get(bucketIndex).size()){
              // this should never happen!
              LOG.warn("Number of ProducerRecord does not match the number of RecordMetadata, "
                  + "LogName:{}, Topic:{}, BucketIndex:{}, result_size:{}, bucket_size:{}",
                  logName, topic, bucketIndex, result.getRecordMetadataList().size(),
                  buckets.get(bucketIndex).size());
              OpenTsdbMetricConverter.incr(SingerMetrics.AUDIT_HEADERS_METADATA_COUNT_MISMATCH, 1,
                  "topic=" + topic, "host=" + HOSTNAME, "logStreamName=" + logName, "partition=" + bucketIndex);
            } else {
              // regular code execution path
              enqueueLoggingAuditEvents(result, mapOfHeadersMap.get(bucketIndex));
              OpenTsdbMetricConverter.incr(SingerMetrics.AUDIT_HEADERS_METADATA_COUNT_MATCH, 1,
                  "host=" + HOSTNAME, "logStreamName=" + logName);
            }
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

  private boolean isLoggingAuditEnabledAndConfigured(){
    return this.enableLoggingAudit && SingerSettings.getLoggingAuditClient() != null;
  }

  public void enqueueLoggingAuditEvents(KafkaWritingTaskResult result,
                                        Map<Integer, LoggingAuditHeaders> headersMap){
    for (Map.Entry<Integer, LoggingAuditHeaders> entry : headersMap.entrySet()) {
        Integer listIndex = entry.getKey();
        LoggingAuditHeaders loggingAuditHeaders = entry.getValue();
        long messageAcknowledgedTimestamp = -1;
        RecordMetadata recordMetadata =result.getRecordMetadataList().get(listIndex);
        messageAcknowledgedTimestamp = recordMetadata.timestamp();
        SingerSettings.getLoggingAuditClient().audit(this.logName, loggingAuditHeaders,
            true, messageAcknowledgedTimestamp, kafkaClusterSig, topic);
    }
  }

  public void close() throws IOException {
    // We should not close the producer here since the producer might be shared by multiple
    // LogStreamWriter.
  }
}
