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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.LogStreamWriter;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.common.errors.LogStreamWriterException;
import com.pinterest.singer.loggingaudit.client.AuditHeadersGenerator;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditHeaders;
import com.pinterest.singer.loggingaudit.thrift.configuration.AuditConfig;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.configuration.MemqWriterConfig;
import com.pinterest.singer.utils.SingerUtils;
import com.pinterest.singer.writer.memq.commons.Compression;
import com.pinterest.singer.writer.memq.commons.serde.ByteArraySerializer;
import com.pinterest.singer.writer.memq.producer.MemqProducer;
import com.pinterest.singer.writer.memq.producer.MemqWriteResult;
import com.pinterest.singer.writer.memq.producer.MemqProducer.ClientType;

public class MemqWriter implements LogStreamWriter {

  public static final String HOSTNAME = SingerSettings.getEnvironment().getHostname();
  private static final Logger LOG = LoggerFactory.getLogger(MemqWriter.class);
  private MemqProducer<byte[], byte[]> client;
  private LogStream logStream;
  private String topic;
  private Set<Future<MemqWriteResult>> futures;
  private List<LoggingAuditHeaders> auditingHeaders;
  private long ts;
  private boolean enableLoggingAudit;
  private AuditConfig auditConfig;
  private AuditHeadersGenerator auditHeadersGenerator;
  private String logName;
  private TSerializer headerSerializer;
  private String clusterSignature;
  private int writeTimeout = 60_000;
  private MemqWriterConfig config;

  public MemqWriter(LogStream logStream, MemqWriterConfig config) throws ConfigurationException {
    this.config = config;
    this.clusterSignature = config.getServerset();
    this.logStream = logStream;
    this.logName = logStream.getSingerLog().getSingerLogConfig().getName();
    this.topic = config.getTopic();
    this.headerSerializer = new TSerializer();
    try {
      this.client = MemqProducer.getInstance(config.getServerset(), config.getTopic(),
          config.getMaxInFlightRequests(), config.getMaxPayLoadBytes(),
          Compression.valueOf(config.getCompression()), config.isDisableAcks(),
          config.getAckCheckPollInterval(), ClientType.valueOf(config.getClientType()),
          SingerSettings.getEnvironment().getLocality(), new ByteArraySerializer(),
          new ByteArraySerializer());
    } catch (IOException e) {
      throw new ConfigurationException(
          "Failed to initialize Memq writer for configuration " + config.toString(), e);
    }
    if (config.isDisableAcks()) {
      writeTimeout = 240_000;
    }
    if (logStream != null && logStream.getSingerLog().getSingerLogConfig().isEnableLoggingAudit()) {
      this.enableLoggingAudit = true;
      this.auditConfig = logStream.getSingerLog().getSingerLogConfig().getAuditConfig();
      this.auditHeadersGenerator = new AuditHeadersGenerator(SingerUtils.getHostname(), logName);
    }
    LOG.info("Initialized MemqWriter with config:" + config);
  }

  @Override
  public void close() throws IOException {
    client.close();
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
  public void startCommit() throws LogStreamWriterException {
    try {
      this.client = MemqProducer.getInstance(config.getServerset(), config.getTopic(),
          config.getMaxInFlightRequests(), config.getMaxPayLoadBytes(),
          Compression.valueOf(config.getCompression()), config.isDisableAcks(),
          config.getAckCheckPollInterval(), ClientType.valueOf(config.getClientType()),
          SingerSettings.getEnvironment().getLocality(), new ByteArraySerializer(),
          new ByteArraySerializer());
    } catch (IOException e) {
      throw new LogStreamWriterException(
          "Failed to initialize Memq writer for configuration " + config.toString(), e);
    }
    futures = new HashSet<>();
    auditingHeaders = new ArrayList<>();
    ts = System.currentTimeMillis();
  }

  @Override
  public void writeLogMessageToCommit(LogMessage message) throws LogStreamWriterException {
    try {
      OpenTsdbMetricConverter.incr("singer.writer.memq.num_message_written", "topic=" + topic,
          "host=" + HOSTNAME);
      checkAndSetLoggingAuditHeadersForLogMessage(message);
      byte[] serializedHeaders = null;
      LoggingAuditHeaders loggingAuditHeaders = message.getLoggingAuditHeaders();
      if (loggingAuditHeaders != null) {
        serializedHeaders = headerSerializer.serialize(loggingAuditHeaders);
        auditingHeaders.add(loggingAuditHeaders);
      }
      Future<MemqWriteResult> requestFuture = client.writeToTopic(serializedHeaders,
          message.getMessage());
      futures.add(requestFuture);
    } catch (Exception e) {
      throw new LogStreamWriterException("Write for topic:" + topic + " failed", e);
    }
  }

  @Override
  public void endCommit(int numLogMessagesRead) throws LogStreamWriterException {
    try {
      if (!futures.isEmpty()) {
        client.finalizeRequest();
      }
      int bytesWritten = 0;
      int maxAckLatency = 0;
      long timeoutTs = System.currentTimeMillis();
      for (Future<MemqWriteResult> future : futures) {
        try {
          long timeout = writeTimeout + (System.currentTimeMillis() - timeoutTs);
          timeout = timeout <= 1 ? 1 : timeout;
          MemqWriteResult memqWriteResult = future.get(timeout, TimeUnit.MILLISECONDS);
          bytesWritten += memqWriteResult.getBytesWritten();
          if (memqWriteResult.getAckLatency() > maxAckLatency) {
            maxAckLatency = memqWriteResult.getAckLatency();
          }
        } catch (TimeoutException e) {
          throw new LogStreamWriterException("MemqWrite timed out", e);
        }
      }
      ts = System.currentTimeMillis() - ts;
      long messageAcknowledgedTimestamp = System.currentTimeMillis();
      for (LoggingAuditHeaders loggingAuditHeaders : auditingHeaders) {
        SingerSettings.getLoggingAuditClient().audit(this.logName, loggingAuditHeaders, true,
            messageAcknowledgedTimestamp, clusterSignature, topic);
      }
      OpenTsdbMetricConverter.gauge("singer.writer.memq.futures", futures.size(), "topic=" + topic,
          "host=" + HOSTNAME);
      OpenTsdbMetricConverter.gauge("singer.writer.memq.latency", ts, "topic=" + topic,
          "host=" + HOSTNAME);
      OpenTsdbMetricConverter.incr("singer.writer.memq.num_message_delivery_success",
          numLogMessagesRead, "topic=" + topic, "host=" + HOSTNAME);
      OpenTsdbMetricConverter.incr("singer.writer.memq.bytes_written", bytesWritten,
          "topic=" + topic, "host=" + HOSTNAME);
      OpenTsdbMetricConverter.incr("singer.writer.memq.batches", "topic=" + topic,
          "host=" + HOSTNAME);
      LOG.info("Write completed successfully:" + numLogMessagesRead + " delta:" + ts + "ms");
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

  public void checkAndSetLoggingAuditHeadersForLogMessage(LogMessage msg) {
    if (enableLoggingAudit && auditConfig.isStartAtCurrentStage()
        && ThreadLocalRandom.current().nextDouble() < auditConfig.getSamplingRate()) {
      LoggingAuditHeaders loggingAuditHeaders = null;
      try {
        loggingAuditHeaders = auditHeadersGenerator.generateHeaders();
        msg.setLoggingAuditHeaders(loggingAuditHeaders);
        LOG.debug("Setting loggingAuditHeaders {} for {}", loggingAuditHeaders, logName);
        OpenTsdbMetricConverter.incr(SingerMetrics.AUDIT_HEADERS_SET_FOR_LOG_MESSAGE,
            "topic=" + topic, "host=" + HOSTNAME,
            "logName=" + msg.getLoggingAuditHeaders().getLogName(), "logStreamName=" + logName);
      } catch (Exception e) {
        OpenTsdbMetricConverter.incr(SingerMetrics.AUDIT_HEADERS_SET_FOR_LOG_MESSAGE_EXCEPTION,
            "topic=" + topic, "host=" + HOSTNAME,
            "logName=" + msg.getLoggingAuditHeaders().getLogName(), "logStreamName=" + logName);
        LOG.debug("Couldn't set loggingAuditHeaders {} for {} as logging audit is enabled "
            + "and start at Singer {} ", loggingAuditHeaders, logName, auditConfig);
      }
    }
  }
}