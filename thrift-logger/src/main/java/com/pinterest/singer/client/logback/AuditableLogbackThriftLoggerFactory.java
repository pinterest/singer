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

import com.pinterest.singer.client.AuditableLogbackThriftLogger;
import com.pinterest.singer.client.BaseThriftLoggerFactory;
import com.pinterest.singer.client.ThriftLogger;
import com.pinterest.singer.client.ThriftLoggerConfig;
import com.pinterest.singer.thrift.LogMessage;

import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ContextBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory that creates a AuditableLogbackThriftLogger.
 *
 * By default we use a file-rolling appender.
 */
public class AuditableLogbackThriftLoggerFactory extends BaseThriftLoggerFactory {

  private static Logger LOG = LoggerFactory.getLogger(AuditableLogbackThriftLoggerFactory.class);

  private final ContextBase contextBase = new ContextBase();

  @Deprecated
  @Override
  protected synchronized ThriftLogger createLogger(String topic, int maxRetentionHours) {
    throw new UnsupportedOperationException("For AuditableLogbackThriftLoggerFactory, please create "
        + "ThriftLoggerConfig object and use createLogger(ThriftLoggerConfig thriftLoggerConfig)"
        + " to create ThriftLogger ");
  }

  @Override
  protected synchronized ThriftLogger createLogger(ThriftLoggerConfig thriftLoggerConfig) {
    if (thriftLoggerConfig.baseDir == null || thriftLoggerConfig.logRotationThresholdBytes <= 0 ||
        thriftLoggerConfig.kafkaTopic == null || thriftLoggerConfig.thriftClazz == null) {
      throw new IllegalArgumentException("The fields of thriftLoggerConfig are not properly set.");
    }

    Appender<LogMessage> appender = AppenderUtils.createFileRollingThriftAppender(
        thriftLoggerConfig.baseDir,
        thriftLoggerConfig.kafkaTopic,
        thriftLoggerConfig.logRotationThresholdBytes / 1024, // convert to KB
        contextBase,
        thriftLoggerConfig.maxRetentionSecs / (60 * 60));    // lowest granularity is hours

    LOG.info("Create AuditableLogbackThriftLogger based on config: " + thriftLoggerConfig.toString());
    return new AuditableLogbackThriftLogger(appender, thriftLoggerConfig.kafkaTopic,
        thriftLoggerConfig.thriftClazz);
  }
}
