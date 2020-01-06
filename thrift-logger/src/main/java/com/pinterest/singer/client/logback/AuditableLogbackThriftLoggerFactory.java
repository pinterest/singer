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
import com.pinterest.singer.client.ThriftLogger;
import com.pinterest.singer.client.ThriftLoggerConfig;
import com.pinterest.singer.loggingaudit.client.LoggingAuditClient;
import com.pinterest.singer.utils.CommonUtils;
import com.pinterest.singer.loggingaudit.client.utils.ConfigUtils;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditStage;
import com.pinterest.singer.loggingaudit.thrift.configuration.LoggingAuditClientConfig;
import com.pinterest.singer.loggingaudit.thrift.configuration.AuditConfig;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.LogMessage;

import ch.qos.logback.core.Appender;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SubsetConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;


/**
 * Factory that creates a AuditableLogbackThriftLogger.
 *
 * By default we use a file-rolling appender.
 */
public class AuditableLogbackThriftLoggerFactory extends LogbackThriftLoggerFactory{

  private static Logger LOG = LoggerFactory.getLogger(AuditableLogbackThriftLoggerFactory.class);

  private static final String AUDIT_THRIFT_LOGGER_CLIENT_INIT_EXCEPTION = "audit.thrift_logger.audit_client.init.exception";
  private static LoggingAuditClient loggingAuditClient;

  @Deprecated
  public AuditableLogbackThriftLoggerFactory(File basePath, int rotateThresholdKBytes) {
    super(basePath, rotateThresholdKBytes);
  }

  public AuditableLogbackThriftLoggerFactory() {
    super();
  }

  public static void createLoggingAuditClientFromConfigFile(String filePath){
    if (loggingAuditClient != null){
      return;
    }
    try {
      LoggingAuditClientConfig config = ConfigUtils.parseFileBasedLoggingAuditClientConfig(filePath);
      loggingAuditClient = new LoggingAuditClient(config);
    } catch (ConfigurationException e){
      loggingAuditClient = null;
      LOG.error("Couldn't successfully create LoggingAuditClient and thus LoggingAuditEvent can not be sent out.");
      OpenTsdbMetricConverter.incr(AUDIT_THRIFT_LOGGER_CLIENT_INIT_EXCEPTION , "stage=" + LoggingAuditStage.THRIFTLOGGER.toString(), "host=" + CommonUtils.getHostName());
    }
  }

  public static void createLoggingAuditClientFromMap(Map<String, String> properties){
    if (loggingAuditClient != null){
      return;
    }
    try {
      LoggingAuditClientConfig config = ConfigUtils.createLoggingAuditClientConfigFromKVs(properties);
      loggingAuditClient = new LoggingAuditClient(config);
    } catch (ConfigurationException e){
      loggingAuditClient = null;
      LOG.error("Couldn't successfully create LoggingAuditClient and thus LoggingAuditEvent can not be sent out.");
      OpenTsdbMetricConverter.incr(AUDIT_THRIFT_LOGGER_CLIENT_INIT_EXCEPTION , "stage=" + LoggingAuditStage.THRIFTLOGGER.toString(), "host=" + CommonUtils.getHostName());
    }
  }

  public static void createAuditConfigFromSingerLogConfigFile(String topic, String singerLogConfigFile){
    if (loggingAuditClient == null){
      return;
    }
    try {
      PropertiesConfiguration conf = new PropertiesConfiguration(singerLogConfigFile);
      AuditConfig auditConfig = ConfigUtils.parseAuditConfig(new SubsetConfiguration(conf,  "loggingaudit.thriftlogger."));
      loggingAuditClient.addAuditConfig(topic, auditConfig);
    } catch (ConfigurationException e){
      LOG.error("Couldn't successfully create AuditConfig for a topic");
    }
  }

  public static LoggingAuditClient getLoggingAuditClient(){
    return loggingAuditClient;
  }


  /**
   * If this method is called, AuditableLogbackThriftLoggerFactory will create a normal
   * LogbackThriftLogger instance which is same as what LogbackThriftLoggerFactory creates.
   * LoggingAudit feature is not enabled.
   *
   * @param topic
   * @param maxRetentionHours
   * @return LogbackThriftLogger instance
   */
  @Deprecated
  @Override
  protected synchronized ThriftLogger createLogger(String topic, int maxRetentionHours) {
    return super.createLogger(topic, maxRetentionHours);
  }

  @Override
  protected synchronized ThriftLogger createLogger(ThriftLoggerConfig thriftLoggerConfig) {
    if (thriftLoggerConfig.getBaseDir() == null || thriftLoggerConfig.getLogRotationThresholdBytes() <= 0 ||
        thriftLoggerConfig.getKafkaTopic() == null ) {
      throw new IllegalArgumentException("The fields of thriftLoggerConfig are not properly set.");
    }

    Appender<LogMessage> appender = AppenderUtils.createFileRollingThriftAppender(
        thriftLoggerConfig.getBaseDir(),
        thriftLoggerConfig.getKafkaTopic(),
        thriftLoggerConfig.getLogRotationThresholdBytes()/ 1024, // convert to KB
        contextBase,
        thriftLoggerConfig.getMaxRetentionSecs() / (60 * 60));    // lowest granularity is hours

    if (loggingAuditClient != null && (thriftLoggerConfig.getThriftClazz()  != null || thriftLoggerConfig.isEnableLoggingAudit())){
      Map<String, String> properties = createProperties(thriftLoggerConfig.getAuditSamplingRate());
      AuditConfig auditConfig = loggingAuditClient.addAuditConfigFromMap(thriftLoggerConfig.getKafkaTopic(), properties);
      LOG.info("Add AuditConfig ({}) for {} ",  auditConfig, thriftLoggerConfig.getKafkaTopic());
    }

    LOG.info("Create AuditableLogbackThriftLogger based on config: " + thriftLoggerConfig.toString());
    return new AuditableLogbackThriftLogger(appender, thriftLoggerConfig.getKafkaTopic(),
        thriftLoggerConfig.getThriftClazz(), thriftLoggerConfig.isEnableLoggingAudit(),
        thriftLoggerConfig.getAuditSamplingRate());
  }

  private Map<String, String> createProperties(double auditSamplingRate){
    return new HashMap<String, String>() {
      {
        put("startAtCurrentStage", "true");
        put("stopAtCurrentStage", "false");
        put("samplingRate", String.valueOf(auditSamplingRate));
      }
    };
  }

  @Override
  public synchronized void shutdown() {
    super.shutdown();
    if (loggingAuditClient != null) {
      loggingAuditClient.close();
      LOG.info("LoggingAuditClient is closed gracefully.");
    }
  }

}
