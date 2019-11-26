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
import com.pinterest.singer.client.BaseThriftLoggerFactory;
import com.pinterest.singer.client.LogbackThriftLogger;
import com.pinterest.singer.client.ThriftLogger;
import com.pinterest.singer.client.ThriftLoggerConfig;

import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ContextBase;

import java.io.File;

/**
 * Factory that creates a logback-based logger.
 *
 * By default we use a file-rolling appender.
 */
public class LogbackThriftLoggerFactory extends BaseThriftLoggerFactory {

  protected final File basePath;
  protected final int rotateThresholdKBytes;
  protected final ContextBase contextBase = new ContextBase();

  @Deprecated
  public LogbackThriftLoggerFactory(File basePath, int rotateThresholdKBytes) {
    this.basePath = basePath;
    this.rotateThresholdKBytes = rotateThresholdKBytes;
  }

  public LogbackThriftLoggerFactory() {
    basePath = null;
    rotateThresholdKBytes = -1;
  }

  @Deprecated
  @Override
  protected synchronized ThriftLogger createLogger(String topic, int maxRetentionHours) {
    if (basePath == null || rotateThresholdKBytes <= 0) {
      throw new IllegalArgumentException(
          "basePath or rotateThresholdKBytes are invalid. Please pass in a ThriftLoggerConfig.");
    }

    return new LogbackThriftLogger(AppenderUtils.createFileRollingThriftAppender(
        basePath,
        topic,
        rotateThresholdKBytes,
        contextBase,
        maxRetentionHours));
  }

  @Override
  protected synchronized ThriftLogger createLogger(ThriftLoggerConfig thriftLoggerConfig) {
    Appender<LogMessage> appender = AppenderUtils.createFileRollingThriftAppender(
        thriftLoggerConfig.baseDir,
        thriftLoggerConfig.kafkaTopic,
        thriftLoggerConfig.logRotationThresholdBytes / 1024, // convert to KB
        contextBase,
        thriftLoggerConfig.maxRetentionSecs / (60 * 60));    // lowest granularity is hours

    return new LogbackThriftLogger(thriftLoggerConfig.kafkaTopic, appender);
  }
}
