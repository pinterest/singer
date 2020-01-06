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

import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.LogMessage;

import ch.qos.logback.core.Appender;
import ch.qos.logback.core.LogbackException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import java.net.InetAddress;

/**
 * LogbackThriftLogger is a class to log to Singer.
 *
 * Logback is used under the hood to log to and rotate the underlying
 * file. The file is written using TFramedTransport as a format, so
 * it is compatible with Singer's expected format.
 */
public class LogbackThriftLogger extends BaseThriftLogger {

  protected static final String THRIFT_LOGGER_COUNT_METRIC = "thrift_logger.count";
  protected static final String THRIFT_LOGGER_ERROR_TEXCEPTION = "thrift_logger.error.texception";
  protected static final String THRIFT_LOGGER_ERROR_LOGBACKEXCEPTION
      = "thrift_logger.error.logbackexception";

  protected static String HOST_NAME = "n/a";
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

  protected final Appender<LogMessage> appender;
  protected final String topic;

  @Deprecated
  public LogbackThriftLogger(Appender<LogMessage> appender) {
    this.appender = appender;
    this.topic = "unknown";
  }

  public LogbackThriftLogger(String topic, Appender<LogMessage> appender) {
    this.topic = topic;
    this.appender = appender;
  }

  /**
   * This method is also called in AuditableLogackThriftLogger to minimize boilerplate code.
   * @param logMessage  logMessage
   * @param partitionKey partition key
   */
  protected void append(LogMessage logMessage, byte[] partitionKey) {
    try {
      if (partitionKey != null) {
        logMessage.setKey(partitionKey);
      }
      appender.doAppend(logMessage);
      OpenTsdbMetricConverter.incr(
          THRIFT_LOGGER_COUNT_METRIC, "topic=" + topic, "host=" + HOST_NAME);
    } catch (LogbackException e) {
      OpenTsdbMetricConverter.incr(
          THRIFT_LOGGER_ERROR_LOGBACKEXCEPTION, "topic=" + topic, "host=" + HOST_NAME);
      throw e;
    }
  }

  @Override
  public void append(byte[] partitionKey, byte[] message, long timeNanos) {
      LogMessage logMessage = new LogMessage().setTimestampInNanos(timeNanos).setMessage(message);
      append(logMessage, partitionKey);
  }

  @Override
  public void append(byte[] partitionKey, TBase thriftMessage, long timeNanos) throws TException {
    try {
      byte[] messageBytes = ThriftCodec.getInstance().serialize(thriftMessage);
      append(partitionKey, messageBytes, timeNanos);
    } catch (TException e) {
      OpenTsdbMetricConverter.incr(
          THRIFT_LOGGER_ERROR_TEXCEPTION, "topic=" + topic, "host=" + HOST_NAME);
      throw e;
    }
  }

  public void close() {
    appender.stop();
  }
}
