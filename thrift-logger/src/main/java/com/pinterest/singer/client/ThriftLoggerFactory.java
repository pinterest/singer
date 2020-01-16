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
import com.pinterest.singer.client.logback.LogbackThriftLoggerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * ThriftLogger factory takes care of managing the loggers for each topic. In the current
 * implementation we only limit one logger per topic. If multiple loggers are created for the same
 * topic with different configurations the logger will be created with the first configuration. So,
 * the uniqueness of a logger is ensured by the kafka topic name alone. Most of the functionality
 * in this class is in the base class. Use the logger as follows:
 *
 * ThriftLoggerFactory.initialize()
 * ThriftLogger myLogger = ThriftLoggerFactory.getLogger(new ThriftLoggerConfig(...));
 */
public class ThriftLoggerFactory {

  private static Logger LOGGER = LoggerFactory.getLogger(ThriftLoggerFactory.class);
  private static ThriftLoggerFactoryInterface THRIFT_LOGGER_FACTORY_INSTANCE;
  private static final String SUCCESSFUL_INITIALIZATION_MSG =
      "Initialized ThriftLoggerFactory to use ";
  private static String LOGGER_MSG_OUT_DIR =
      "Initalizing thrift logger factory to output directory:";

  /**
   * An interface to abstract out logger creation.
   */
  public interface ThriftLoggerFactoryInterface {

    @Deprecated
    public ThriftLogger getLogger(String topic, int maxRetentionHours);

    public ThriftLogger getLogger(ThriftLoggerConfig thriftLoggerConfig);

    // Call to shutdown the factory and close all logs.
    void shutdown();
  }

  /**
   * Refactoring: Let initialize() method create AuditableLogbackThriftLoggerFactory by default.
   * AuditableLogbackThriftLoggerFactory will create AuditableLogbackThriftLogger if audit
   * configurations are used, otherwise it will create LogbackThriftLogger. Call this at the start
   * of your server to initialize the thrift logging layer.
   */
  public static synchronized void initialize(File outputDirectory, int rotateThresholdKilobytes) {
    if (THRIFT_LOGGER_FACTORY_INSTANCE != null) {
      LOGGER.info("Already initialized factory instance. Not initializing another instance.");
      return;
    }

    if (outputDirectory == null) {
       THRIFT_LOGGER_FACTORY_INSTANCE = new AuditableLogbackThriftLoggerFactory();
    } else {
      THRIFT_LOGGER_FACTORY_INSTANCE = new AuditableLogbackThriftLoggerFactory(outputDirectory,
          rotateThresholdKilobytes);
    }
    LOGGER.info(SUCCESSFUL_INITIALIZATION_MSG + THRIFT_LOGGER_FACTORY_INSTANCE);

    // Add a hook to shutdown loggers on program exit.
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        THRIFT_LOGGER_FACTORY_INSTANCE.shutdown();
      }
    });
  }

  public static synchronized void initialize() {
    initialize(null, -1);
  }

  /**
   * Create a Noop factory which doesn't log anything. Only useful in unit tests.
   */
  public static synchronized void initializeNoopFactory() {
    if (THRIFT_LOGGER_FACTORY_INSTANCE != null) {
      return;
    }
    LOGGER.info(LOGGER_MSG_OUT_DIR);
    THRIFT_LOGGER_FACTORY_INSTANCE = new ThriftLoggerFactoryInterface() {
      @Deprecated
      @Override
      public ThriftLogger getLogger(String topic, int maxRetentionHours) {
        return new NoopLogger();
      }

      @Override
      public ThriftLogger getLogger(ThriftLoggerConfig thriftLoggerConfig) {
        return new NoopLogger();
      }

      @Override
      public void shutdown() { }
    };
    LOGGER.info(SUCCESSFUL_INITIALIZATION_MSG + THRIFT_LOGGER_FACTORY_INSTANCE);
  }

  /**
   * Use a custom factory, which is currently only used for unit tests.
   */
  public static synchronized void initializeCustomFactory(ThriftLoggerFactoryInterface factory) {
    if (THRIFT_LOGGER_FACTORY_INSTANCE != null) {
      return;
    }
    LOGGER.info("Initializing custom ThriftLoggerFactory.");
    THRIFT_LOGGER_FACTORY_INSTANCE = factory;
    LOGGER.info(SUCCESSFUL_INITIALIZATION_MSG + THRIFT_LOGGER_FACTORY_INSTANCE);
  }

  @Deprecated
  public static ThriftLogger getLogger(String topic, int maxRetentionHours) {
    if (THRIFT_LOGGER_FACTORY_INSTANCE == null) {
      throw new IllegalStateException("ThriftLoggerFactory.initialize() not called.");
    }
    return THRIFT_LOGGER_FACTORY_INSTANCE.getLogger(topic, maxRetentionHours);
  }

  public static ThriftLogger getLogger(ThriftLoggerConfig thriftLoggerConfig) {
    if (THRIFT_LOGGER_FACTORY_INSTANCE == null) {
      throw new IllegalStateException("ThriftLoggerFactory.initialize() not called.");
    }
    return THRIFT_LOGGER_FACTORY_INSTANCE.getLogger(thriftLoggerConfig);
  }

  public static ThriftLoggerFactoryInterface getThriftLoggerFactoryInstance() {
    return THRIFT_LOGGER_FACTORY_INSTANCE;
  }
}
