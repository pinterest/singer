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
package com.pinterest.singer.client.logback;

import com.pinterest.singer.thrift.LogMessage;

import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.encoder.EncoderBase;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeAndTimeBasedFNATP;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import ch.qos.logback.core.util.FileSize;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Utils to create logback appenders
 */
public class AppenderUtils {

  public static final String PATH_SEP = "/";

  private AppenderUtils() {
  }

  /**
   * Encoder for LogMessage objects.
   */
  public static class LogMessageEncoder extends EncoderBase<LogMessage> {

    private TTransport framedTransport;
    private TProtocol protocol;
    private OutputStream os;

    @Override
    public void init(OutputStream os) {
      this.os = os;
      // Use the TFlushingFastFramedTransport to be compatible with singer_thrift
      // log.
      final int bufferCapacity = 10;
      framedTransport = new TFastFramedTransport(new TIOStreamTransport(os),
          bufferCapacity);
      protocol = new TBinaryProtocol(framedTransport);
    }

    @Override
    public void doEncode(LogMessage logMessage) throws IOException {
      try {
        logMessage.write(protocol);
        framedTransport.flush();
      } catch (TException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws IOException {
      framedTransport.close();
    }
  }

  /**
   * Create the basic thrift appender which logs to a file
   * and rolls the file when it exceeds a certain size.
   *
   * @param basePath base directory the files are under.
   * @param topic the topic name for the current appender.
   * @param rotateThresholdKBytes threshold in kilobytes to rotate after.
   * @param context the logback context.
   */
  public static Appender<LogMessage> createFileRollingThriftAppender(
      File basePath,
      String topic,
      long rotateThresholdKBytes,
      Context context,
      int maxRetentionHours) {
    RollingFileAppender<LogMessage> appender = new RollingFileAppender<LogMessage>();
    appender.setContext(context);
    appender.setAppend(true);
    appender.setPrudent(false);

    LogMessageEncoder encoder = new LogMessageEncoder();
    appender.setEncoder(encoder);
    appender.setFile(basePath + PATH_SEP + topic);

    TimeBasedRollingPolicy policy = new TimeBasedRollingPolicy();
    policy.setMaxHistory(maxRetentionHours);
    policy.setFileNamePattern(basePath + PATH_SEP + topic + ".%d{yyyy-MM-dd-HH}.%i");
    policy.setCleanHistoryOnStart(false);
    policy.setContext(context);
    policy.setParent(appender);

    // Also impose a max size per file policy.
    SizeAndTimeBasedFNATP fnatp = new SizeAndTimeBasedFNATP();
    fnatp.setContext(context);
    fnatp.setTimeBasedRollingPolicy(policy);
    fnatp.setMaxFileSize(FileSize.valueOf(String.format("%sKB", rotateThresholdKBytes)));

    policy.setTimeBasedFileNamingAndTriggeringPolicy(fnatp);
    appender.setRollingPolicy(policy);
    appender.setTriggeringPolicy(policy);

    policy.start();
    appender.start();

    return appender;
  }
}
