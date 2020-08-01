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
package com.pinterest.singer.client;

import com.pinterest.singer.client.logback.AuditableLogbackThriftLoggerFactory;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditHeaders;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.loggingaudit.client.AuditHeadersGenerator;

import ch.qos.logback.core.Appender;
import ch.qos.logback.core.LogbackException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.zip.CRC32;


/**
 * AuditableLogbackThriftLogger is a class to log to Singer.
 *
 * Logback is used under the hood to log to and rotate the underlying
 * file. The file is written using TFramedTransport as a format, so
 * it is compatible with Singer's expected format.
 */
public class AuditableLogbackThriftLogger extends LogbackThriftLogger {

  private static Logger LOG = LoggerFactory.getLogger(AuditableLogbackThriftLogger.class);
  private static ThreadLocal<CRC32> localCRC = ThreadLocal.withInitial(CRC32::new);

  private static final String AUDIT_THRIFT_LOGGER_HEADERS_ADDED_TO_ORIGINAL_COUNT = "audit.thrift_logger.headers_added_to_original.count";
  private static final String AUDIT_THRIFT_LOGGER_HEADERS_ADDED_TO_LOG_MESSAGE_COUNT = "audit.thrift_logger.headers_added_to_log_message.count";
  private static final String AUDIT_THRIFT_LOGGER_AUDITED_MESSAGE_COUNT = "audit.thrift_logger.audited_message.count";
  private static final String AUDIT_THRIFT_LOGGER_ERROR_INIT = "audit.thrift_logger.error.init";

  private Class<?> thriftClazz;
  private TFieldIdEnum loggingAuditHeadersField;
  private AuditHeadersGenerator auditHeadersGenerator;
  private boolean enableLoggingAudit = false;
  private double auditSamplingRate = 1.0;
  private Random random = new Random();

  public void setAuditHeadersGenerator(AuditHeadersGenerator auditHeadersGenerator) {
    this.auditHeadersGenerator = auditHeadersGenerator;
  }

  public boolean isEnableLoggingAudit() {
    return enableLoggingAudit;
  }

  public double getAuditSamplingRate() {
    return auditSamplingRate;
  }


  /**
   *  If thriftClazz is not null, advanced LoggingAudit feature is enabled (enableLoggingAudit is true):
   *  LoggingAuditHeaders will be set not only in LogMessage but also in the original message assuming
   *  the thrift struct of the original message has LoggingAuditHeaders as one of the optional field.
   *
   *  If thriftClazz is null and enableLoggingAudit is true, normal LoggingAudit feature is enabled:
   *  LoggingAuditHeaders will be set in LogMessage only.
   *
   *  If enableLoggingAudit is false, LoggingAudit feature is disabled and
   *  AuditableLogbackThriftLogger behaves just like the normal LogbackThriftLogger.
   *
   *  Compared to the normal LoggingAudit feature, advanced LoggingAudit feature can inject
   *  LoggingAuditHeaders into the original message and could be applicable for various use cases
   *  such as debugging and tracing. This is the recommended way to enable LoggingAudit feature.
   *
   */

  public AuditableLogbackThriftLogger(Appender<LogMessage> appender,
                                      String topic,
                                      Class<?> thriftClazz,
                                      boolean enableLoggingAudit,
                                      double auditSamplingRate) {
    super(topic, appender);
    this.auditHeadersGenerator = new AuditHeadersGenerator(HOST_NAME, topic);
    this.thriftClazz = thriftClazz;
    if (this.thriftClazz != null) {
      this.enableLoggingAudit = true;
      init(this.thriftClazz);
    } else {
      this.enableLoggingAudit = enableLoggingAudit;
    }
    this.auditSamplingRate = auditSamplingRate;
  }

  public void init(Class<?> thriftClazz) {
    try {
      Map m = (Map) thriftClazz.getDeclaredField("metaDataMap").get(null);
      for (Object o : m.keySet()) {
        TFieldIdEnum tf = (TFieldIdEnum) o;
        if ("loggingAuditHeaders".equalsIgnoreCase(tf.getFieldName())) {
          this.loggingAuditHeadersField = tf;
          LOG.info("Found loggingAuditHeaders field for {}", thriftClazz.getName());
          break;
        }
      }
      if (this.loggingAuditHeadersField == null) {
        throw new NoSuchFieldException(
            "Cannot find loggingAuditHeaders field for " + thriftClazz.getName());
      }
    } catch (Exception e) {
      OpenTsdbMetricConverter.incr(AUDIT_THRIFT_LOGGER_ERROR_INIT, "topic=" + topic,
          "host=" + HOST_NAME);
      LOG.error("Init method cannot finish successfully due to {}", e.getMessage());
    }
  }

  @Override
  public void append(byte[] partitionKey, byte[] message, long timeNanos) {
    LoggingAuditHeaders headers = null;
    if (enableLoggingAudit) {
      headers = auditHeadersGenerator.generateHeaders();
    }
    append(partitionKey, message,timeNanos, headers);
  }

  @Override
  public void append(byte[] partitionKey, TBase thriftMessage, long timeNanos) throws TException {
    LoggingAuditHeaders headers = null;
    try {
      if (enableLoggingAudit && shouldAudit()) {
        headers = auditHeadersGenerator.generateHeaders();
        if (this.loggingAuditHeadersField != null) {
          thriftMessage.setFieldValue(this.loggingAuditHeadersField, headers);
          OpenTsdbMetricConverter.incr(AUDIT_THRIFT_LOGGER_HEADERS_ADDED_TO_ORIGINAL_COUNT,
              "topic=" + topic, "host=" + HOST_NAME);
        }
      }
      byte[] messageBytes = ThriftCodec.getInstance().serialize(thriftMessage);
      append(partitionKey, messageBytes, timeNanos, headers);
    } catch (TException e) {
      OpenTsdbMetricConverter.incr(THRIFT_LOGGER_ERROR_TEXCEPTION,
          "topic=" + topic, "host=" + HOST_NAME);
      throw e;
    }
  }

  private void append(byte[] partitionKey, byte[] message, long timeNanos,
                              LoggingAuditHeaders headers) throws LogbackException {
      LogMessage logMessage = new LogMessage()
          .setTimestampInNanos(timeNanos).setMessage(message);

      if(this.enableLoggingAudit && headers != null) {
        long crc = computeCRC(message);
        logMessage.setLoggingAuditHeaders(headers).setChecksum(crc);
        OpenTsdbMetricConverter.incr(AUDIT_THRIFT_LOGGER_HEADERS_ADDED_TO_LOG_MESSAGE_COUNT,
            "topic=" + topic, "host=" + HOST_NAME);
      }
      super.append(logMessage, partitionKey);
      if (this.enableLoggingAudit && headers != null && AuditableLogbackThriftLoggerFactory.getLoggingAuditClient() != null) {
          AuditableLogbackThriftLoggerFactory.getLoggingAuditClient().audit(this.topic, headers, true,
              System.currentTimeMillis());
          OpenTsdbMetricConverter.incr(AUDIT_THRIFT_LOGGER_AUDITED_MESSAGE_COUNT,
              "topic=" + topic, "host=" + HOST_NAME);
      }
  }

  private long computeCRC(byte[] message) {
    CRC32 crc = localCRC.get();
    crc.reset();
    crc.update(message);
    return crc.getValue();
  }

  public boolean shouldAudit(){
     return random.nextDouble() < auditSamplingRate;
  }

}
