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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base ThriftLogger factory takes care of managing the loggers for each topic. In the current
 * implementation we only limit one logger per topic. If multiple loggers are created for the same
 * topic with different configurations the logger will be created with the first configuration. So,
 * the uniqueness of a logger is ensured by the kafka topic name alone.
 */
public abstract class BaseThriftLoggerFactory
    implements ThriftLoggerFactory.ThriftLoggerFactoryInterface {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseThriftLoggerFactory.class);

  protected ConcurrentHashMap<String, ThriftLogger> loggersByTopic
      = new ConcurrentHashMap<String, ThriftLogger>();

  private int sleepInSecBeforeCloseLoggers = -1;

  public int getSleepInSecBeforeCloseLoggers() {
    return sleepInSecBeforeCloseLoggers;
  }

  public void setSleepInSecBeforeCloseLoggers(int sleepInSecBeforeCloseLoggers) {
    this.sleepInSecBeforeCloseLoggers = sleepInSecBeforeCloseLoggers;
  }

  @Deprecated
  public ThriftLogger getLogger(String topic, int maxRetentionHours) {
    ThriftLogger existingLogger = loggersByTopic.get(topic);
    if (existingLogger != null) {
      return existingLogger;
    }

    return getOrCreateLogger(topic, maxRetentionHours);
  }

  public ThriftLogger getLogger(ThriftLoggerConfig thriftLoggerConfig) {
    ThriftLogger existingLogger = loggersByTopic.get(thriftLoggerConfig.kafkaTopic);
    if (existingLogger != null) {
      return existingLogger;
    }

    return getOrCreateLogger(thriftLoggerConfig);
  }

  @Deprecated
  private synchronized ThriftLogger getOrCreateLogger(String topic, int maxRetentionHours) {
    // This is called by getLogger above only when there's no logger
    // for this topic, to make sure the logger creation is threadsafe.
    // First check again if the logger has been initialized by now.
    ThriftLogger existingLogger = loggersByTopic.get(topic);
    if (existingLogger != null) {
      LOGGER.info("Returning an existing logger for topic: " + topic);
      return existingLogger;
    }

    LOGGER.info("Creating a new logger for topic " + topic);
    ThriftLogger newLogger = createLogger(topic, maxRetentionHours);
    loggersByTopic.put(topic, newLogger);
    return newLogger;
  }

  private synchronized ThriftLogger getOrCreateLogger(ThriftLoggerConfig thriftLoggerConfig) {
    // This is called by getLogger above only when there's no logger
    // for this topic, to make sure the logger creation is threadsafe.
    // First check again if the logger has been initialized by now.
    ThriftLogger existingLogger = loggersByTopic.get(thriftLoggerConfig.kafkaTopic);
    if (existingLogger != null) {
      LOGGER.info("Returning an existing logger for topic: " + thriftLoggerConfig.kafkaTopic);
      return existingLogger;
    }

    ThriftLogger newLogger = createLogger(thriftLoggerConfig);
    loggersByTopic.put(thriftLoggerConfig.kafkaTopic, newLogger);
    LOGGER.info(String.format("Created a  new logger for topic %s with config %s",
        thriftLoggerConfig.kafkaTopic, thriftLoggerConfig.toString()));
    return newLogger;
  }

  @Deprecated
  protected abstract ThriftLogger createLogger(String topic, int maxRetentionHours);

  protected abstract ThriftLogger createLogger(ThriftLoggerConfig thriftLoggerConfig);

  public synchronized void shutdown() {
    try {
      if (sleepInSecBeforeCloseLoggers > 0) {
        LOGGER.info("Before closing loggers, sleep {} seconds", sleepInSecBeforeCloseLoggers);
        Thread.sleep(sleepInSecBeforeCloseLoggers * 1000);
        LOGGER.info("After {} seconds, start to close loggers", sleepInSecBeforeCloseLoggers);
      }
    } catch (InterruptedException e) {
      LOGGER.warn("Thread got interrupted", e);
    } finally {
      for (Map.Entry<String, ThriftLogger> entry : loggersByTopic.entrySet()) {
        entry.getValue().close();
        LOGGER.info("Logger for topic {} is closed.", entry.getKey());
      }
    }
  }
}
