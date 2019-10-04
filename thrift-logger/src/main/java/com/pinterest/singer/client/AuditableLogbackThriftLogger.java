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

import java.net.InetAddress;
import java.util.Map;


/**
 * AuditableLogbackThriftLogger is a class to log to Singer.
 *
 * Logback is used under the hood to log to and rotate the underlying
 * file. The file is written using TFramedTransport as a format, so
 * it is compatible with Singer's expected format.
 */
public class AuditableLogbackThriftLogger extends BaseThriftLogger {

  private static Logger LOG = LoggerFactory.getLogger(AuditableLogbackThriftLogger.class);

  private static final String AUDIT_THRIFT_LOGGER_COUNT_METRIC = "audit.thrift_logger.count";
  private static final String AUDIT_THRIFT_LOGGER_ERROR_TEXCEPTION = "audit.thrift_logger.error.texception";
  private static final String AUDIT_THRIFT_LOGGER_ERROR_LOGBACKEXCEPTION = "audit.thrift_logger.error.logbackexception";
  private static final String AUDIT_THRIFT_LOGGER_ERROR_INIT = "audit.thrift_logger.error.init";
  private static String HOST_NAME;

  static {
    try {
      String hostName = InetAddress.getLocalHost().getHostName();
      int firstDotPos = hostName.indexOf('.');
      if (firstDotPos > 0) {
        HOST_NAME = hostName.substring(0, firstDotPos);
      }
    } catch (Exception e) {
      // fall back to env var.
      HOST_NAME = System.getenv("HOSTNAME");
    }
  }

  private final Appender<LogMessage> appender;
  private final String topic;
  private Class<?> thriftClazz;
  private TFieldIdEnum loggingAuditHeadersField;
  private AuditHeadersGenerator auditHeadersGenerator;

  public void setAuditHeadersGenerator(AuditHeadersGenerator auditHeadersGenerator) {
    this.auditHeadersGenerator = auditHeadersGenerator;
  }

  public AuditableLogbackThriftLogger(Appender<LogMessage> appender, String topic,
                                      Class<?> thriftClazz) {
    if (thriftClazz == null) {
      throw new IllegalArgumentException(
          "To initialize AuditableLogbackThriftLogger, thrift class must be valid class object.");
    }
    this.appender = appender;
    this.topic = topic;
    this.auditHeadersGenerator = new AuditHeadersGenerator(HOST_NAME, topic);
    this.thriftClazz = thriftClazz;
    init(this.thriftClazz);
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
      LOG.error("Init method cannot finish successfully due to {}, needs to exit.", e.getMessage());
      System.exit(1);
    }
  }

  @Override
  public void append(byte[] partitionKey, byte[] message, long timeNanos) {
    appendInternal(partitionKey, message,timeNanos, auditHeadersGenerator.generateHeaders());
  }

  @Override
  public void append(byte[] partitionKey, TBase thriftMessage, long timeNanos) throws TException {
    try {
      LoggingAuditHeaders headers = auditHeadersGenerator.generateHeaders();
      thriftMessage.setFieldValue(this.loggingAuditHeadersField, headers);
      byte[] messageBytes = ThriftCodec.getInstance().serialize(thriftMessage);
      appendInternal(partitionKey, messageBytes, timeNanos, headers);
    } catch (TException e) {
      OpenTsdbMetricConverter.incr(
          AUDIT_THRIFT_LOGGER_ERROR_TEXCEPTION, "topic=" + topic, "host=" + HOST_NAME);
      throw e;
    }
  }

  private void appendInternal(byte[] partitionKey, byte[] message, long timeNanos,
                              LoggingAuditHeaders headers) throws LogbackException {
    try {
      LogMessage logMessage = new LogMessage()
          .setTimestampInNanos(timeNanos)
          .setMessage(message)
          .setLoggingAuditHeaders(headers);

      if (partitionKey != null) {
        logMessage.setKey(partitionKey);
      }
      appender.doAppend(logMessage);
      OpenTsdbMetricConverter.incr(
          AUDIT_THRIFT_LOGGER_COUNT_METRIC, "topic=" + topic, "host=" + HOST_NAME);
    } catch (LogbackException e) {
      OpenTsdbMetricConverter.incr(
          AUDIT_THRIFT_LOGGER_ERROR_LOGBACKEXCEPTION, "topic=" + topic, "host=" + HOST_NAME);
      throw e;
    }
  }

  public void close() {
    appender.stop();
  }
}
