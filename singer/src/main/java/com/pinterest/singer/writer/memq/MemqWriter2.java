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
package com.pinterest.singer.writer.memq;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import com.codahale.metrics.MetricRegistry;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.internal.OutOfDirectMemoryError;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.client.commons.serde.ByteArraySerializer;
import com.pinterest.memq.client.producer2.MemqProducer;
import com.pinterest.memq.client.producer.MemqWriteResult;
import com.pinterest.memq.commons.MessageId;
import com.pinterest.memq.core.utils.MiscUtils;
import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.LogStreamWriter;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.common.errors.LogStreamWriterException;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditHeaders;
import com.pinterest.singer.loggingaudit.thrift.configuration.AuditConfig;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.LogMessageAndPosition;
import com.pinterest.singer.thrift.LogPosition;
import com.pinterest.singer.thrift.configuration.MemqWriterConfig;

public class MemqWriter2 implements LogStreamWriter {

  public static final String HOSTNAME = SingerSettings.getEnvironment().getHostname();
  private static final Logger LOG = LoggerFactory.getLogger(MemqWriter2.class);
  private static ThreadLocal<CRC32> localCRC = ThreadLocal.withInitial(CRC32::new);
  public static Map<String, MetricRegistry> additionalMetricRegistries = new ConcurrentHashMap<>();
  private MemqProducer<byte[], byte[]> client;
  private LogStream logStream;
  private String topic;
  private Set<Future<MemqWriteResult>> futures;
  private List<LoggingAuditHeaders> auditingHeaders;
  private long commitStartTs;
  private String logName;
  private String clusterSignature;
  private int writeTimeout = 60_000;
  private int requestTimeout = 60_000;
  private MemqWriterConfig config;
  private MemqProducer.Builder<byte[], byte[]> builder;
  private int batchEventCount;
  private boolean enableLoggingAudit = false;
  private AuditConfig auditConfig = null;

  public MemqWriter2(LogStream logStream, MemqWriterConfig config) throws ConfigurationException {
    this.config = config;
    this.clusterSignature = config.getServerset();
    this.logStream = logStream;
    this.logName = logStream.getSingerLog().getSingerLogConfig().getName();
    this.topic = config.getTopic();
    if (logStream.getSingerLog().getSingerLogConfig().isEnableLoggingAudit()){
      this.enableLoggingAudit = true;
      this.auditConfig = logStream.getSingerLog().getSingerLogConfig().getAuditConfig();
    }

    try {
      builder = getProducerBuilder(config);

      this.client = builder.memoize().build();
    } catch (Exception e) {
      throw new ConfigurationException(
          "Failed to initialize Memq writer for configuration " + config.toString(), e);
    }
    if (config.isDisableAcks()) {
      writeTimeout = 240_000;
    }
    LOG.info("Initialized MemqWriter2 with config:" + config);
  }

  private MemqProducer.Builder<byte[], byte[]> getProducerBuilder(MemqWriterConfig config) {
    // we create a tag based on the topic and log name of the producer, and get or create the registry from the pusher
    String metricTag = String.join(" ", "topic=" + topic, "host=" + MiscUtils.getHostname(), "logname=" + logName);
    MetricRegistry registry =
        ((MemQMetricsRegistry) additionalMetricRegistries.compute(metricTag, (k, v) -> v == null ? new MemQMetricsRegistry() : v))
        .getRegistry();
    MemqProducer.Builder<byte[], byte[]> builder = new MemqProducer.Builder<byte[], byte[]>()
        .cluster(config.getCluster())
        .serversetFile(config.getServerset())
        .topic(config.getTopic())
        .maxInflightRequests((config.getMaxInFlightRequests()))
        .lingerMs(config.getAckCheckPollInterval())
        .disableAcks(config.isDisableAcks())
        .maxPayloadBytes(config.getMaxPayLoadBytes())
        .compression(Compression.valueOf(config.getCompression()))
        .sendRequestTimeout(requestTimeout)
        .locality(SingerSettings.getEnvironment().getLocality())
        .keySerializer(new ByteArraySerializer())
        .valueSerializer(new ByteArraySerializer())
        .metricRegistry(registry);
    if (config.getAuditorConfig() != null) {
      Properties auditProperties = new Properties();
      auditProperties.setProperty("enabled", "true");
      auditProperties.setProperty("class", config.getAuditorConfig().getAuditorClass());
      auditProperties.setProperty("serverset", config.getAuditorConfig().getServerset());
      auditProperties.setProperty("topic", config.getAuditorConfig().getTopic());
      builder.auditProperties(auditProperties);
    }
    return builder;
  }

  @Override
  public void close() throws IOException {
//    client.close();
  }

  @Override
  public LogStream getLogStream() {
    return logStream;
  }

  @Override
  public boolean isAuditingEnabled() {
    return false;
  }

  @Override
  public void writeLogMessages(List<LogMessage> messages) throws LogStreamWriterException {
  }

  @Override
  public boolean isCommittableWriter() {
    return true;
  }

  @Override
  public void startCommit(boolean isDraining) throws LogStreamWriterException {
    try {
      this.client = builder.memoize().build();
    } catch (Exception e) {
      throw new LogStreamWriterException(
          "Failed to initialize Memq writer for configuration " + config.toString(), e);
    }
    futures = new HashSet<>();
    auditingHeaders = new ArrayList<>();
    commitStartTs = System.currentTimeMillis();
    batchEventCount = 0;
  }

  private long computeCRC(byte[] message) {
    CRC32 crc = localCRC.get();
    crc.reset();
    crc.update(message);
    return crc.getValue();
  }

  boolean checkMessageValid(LogMessage msg){
    if (msg.getMessage() == null) {
      return false;
    }
    boolean isMessageUncorrupted = true;

    if (msg.isSetChecksum()) {
      long start = System.nanoTime();
      long singerChecksum = computeCRC(msg.getMessage());
      isMessageUncorrupted = singerChecksum == msg.getChecksum();
      OpenTsdbMetricConverter.gauge(SingerMetrics.AUDIT_COMPUTE_CHECKSUM_LATENCY_NANO, Math.max(
          0, System.nanoTime() - start), "host=" + HOSTNAME, "logStreamName=" + logName);
      OpenTsdbMetricConverter.incr(isMessageUncorrupted ? SingerMetrics.AUDIT_NUM_UNCORRUPTED_MESSAGES : SingerMetrics.AUDIT_NUM_CORRUPTED_MESSAGES,
          "host=" + HOSTNAME, "logStreamName=" + logName);
    }
    return isMessageUncorrupted;
  }

  @Override
  public void writeLogMessageToCommit(LogMessageAndPosition messageAndPosition, boolean isDraining) throws LogStreamWriterException {
    LogMessage message = messageAndPosition.getLogMessage();
    // when auditing is enabled, message is not valid and skip corrupted message is true, this
    // message will not be sent to Memq.
    if (enableLoggingAudit && !checkMessageValid(message) && auditConfig.isSkipCorruptedMessageAtCurrentStage()){
      OpenTsdbMetricConverter.incr(SingerMetrics.AUDIT_NUM_INVALID_MESSAGES_SKIPPED, "topic=" + topic,
          "host=" + HOSTNAME, "logStreamName=" + logName);
      return;
    }

    SingerMessageId messageId = new SingerMessageId(messageAndPosition.getNextPosition());
    try {
      OpenTsdbMetricConverter.incr("singer.writer.memq.num_message_written", "topic=" + topic,
          "host=" + HOSTNAME, "drain=" + isDraining);
      byte[] key = null;
      if (message.isSetKey()) {
        key = message.getKey();
      }
      Future<MemqWriteResult> requestFuture = client.write(messageId, key, message.getMessage(), message.getTimestampInNanos());
      if (requestFuture == null) {
        OpenTsdbMetricConverter.incr("singer.writer.memq.dropped", "topic=" + topic,
            "host=" + HOSTNAME, "drain=" + isDraining);
      } else {
        OpenTsdbMetricConverter.incr("singer.writer.memq.raw_bytes_written", message.getMessage().length, "topic=" + topic, "host=" + HOSTNAME, "drain=" + isDraining);
        futures.add(requestFuture);
        batchEventCount++;
      }
    } catch (Exception e) {
      throw new LogStreamWriterException("Write for topic:" + topic + " failed", e);
    } catch (OutOfDirectMemoryError oodme) {
      // we rethrow these as LogStreamWriterExceptions to avoid OODM errors blocking other pipelines
      // this should be fine since only memq uses netty direct memory, so only memq pipelines will be blocked
      // also allows processor to do the retries as usual
      // the following metric needs to be alerted on to make sure we get notified about it
      OpenTsdbMetricConverter.incr("singer.writer.memq.out_of_direct_memory", "topic=" + topic, "host=" + HOSTNAME);
      throw new LogStreamWriterException("Write for topic:" + topic + " failed", oodme);
    }
  }

  @Override
  public void endCommit(int numLogMessagesRead, boolean isDraining) throws LogStreamWriterException {
    try {
      if (!futures.isEmpty()) {
        client.flush();
      }
      int bytesWritten = 0;
      int maxAckLatency = 0;
      long timeoutTs = System.currentTimeMillis();
      for (Future<MemqWriteResult> future : futures) {
        long timeout = writeTimeout + (System.currentTimeMillis() - timeoutTs);
        timeout = timeout <= 1 ? 1 : timeout;
        try {
          MemqWriteResult memqWriteResult = future.get(timeout, TimeUnit.MILLISECONDS);
          bytesWritten += memqWriteResult.getBytesWritten();
          if (memqWriteResult.getAckLatency() > maxAckLatency) {
            maxAckLatency = memqWriteResult.getAckLatency();
          }
        } catch (ExecutionException ee) {
          if (ee.getCause() instanceof OutOfDirectMemoryError) {
            OpenTsdbMetricConverter.incr("singer.writer.memq.out_of_direct_memory", "topic=" + topic, "host=" + HOSTNAME);
          }
          throw ee;
        }
      }
      commitStartTs = System.currentTimeMillis() - commitStartTs;
      long messageAcknowledgedTimestamp = System.currentTimeMillis();
      for (LoggingAuditHeaders loggingAuditHeaders : auditingHeaders) {
        SingerSettings.getLoggingAuditClient().audit(this.logName, loggingAuditHeaders, true,
            messageAcknowledgedTimestamp, clusterSignature, topic);
      }
      OpenTsdbMetricConverter.gauge("singer.writer.memq.futures", futures.size(), "topic=" + topic,
          "host=" + HOSTNAME, "drain=" + isDraining);
      OpenTsdbMetricConverter.gauge("singer.writer.memq.latency", commitStartTs, "topic=" + topic,
          "host=" + HOSTNAME, "drain=" + isDraining);
      OpenTsdbMetricConverter.incr("singer.writer.memq.num_message_delivery_success",
          batchEventCount, "topic=" + topic, "host=" + HOSTNAME, "drain=" + isDraining);
      OpenTsdbMetricConverter.incr("singer.writer.memq.bytes_written", bytesWritten,
          "topic=" + topic, "host=" + HOSTNAME, "drain=" + isDraining);
      OpenTsdbMetricConverter.incr("singer.writer.memq.batches", "topic=" + topic,
          "host=" + HOSTNAME, "drain=" + isDraining);
      OpenTsdbMetricConverter.gauge("singer.writer.memq.direct.memory.usage", PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory(), "host=" + HOSTNAME);
      LOG.info(
          "Write completed successfully:" + numLogMessagesRead + " delta:" + commitStartTs + "ms");
    } catch (Exception e) {
      try {
        client.close();
      } catch (IOException e1) {
        LOG.error("Failed to close client", e1);
      }
      OpenTsdbMetricConverter.incr("singer.writer.memq.failure", "topic=" + topic,
          "host=" + HOSTNAME);
      throw new LogStreamWriterException("Write for topic:" + topic + " failed", e);
    }
  }

  public static class SingerMessageId extends MessageId {

    private static final int SINGER_DEFAULT_MESSAGEID_LENGTH = 16;

    public SingerMessageId(LogPosition position) {
      super(ByteBuffer.wrap(new byte[SINGER_DEFAULT_MESSAGEID_LENGTH])
          .putLong(position.getLogFile().getInode()).putLong(position.getByteOffset()).array());
    }

    public long getInode() {
      return ByteBuffer.wrap(array).getLong();
    }

    public long getOffset() {
      return ByteBuffer.wrap(array).getLong(4);
    }

  }

  // a wrapper registry that adds memq prefix to the name of the metrics
  private static class MemQMetricsRegistry extends MetricRegistry {
    private MetricRegistry registry;
    MemQMetricsRegistry() {
      this.registry = new MetricRegistry();
      this.register("memq", this.registry);
    }

    public MetricRegistry getRegistry() {
      return registry;
    }
  }

}
