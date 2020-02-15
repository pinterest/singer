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
package com.pinterest.singer.utils;

import com.pinterest.singer.common.SingerConfigDef;
import com.pinterest.singer.config.ConfigFileServerSet;
import com.pinterest.singer.config.DirectorySingerConfigurator;
import com.pinterest.singer.environment.EnvironmentProvider;
import com.pinterest.singer.loggingaudit.client.utils.ConfigUtils;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditStage;
import com.pinterest.singer.loggingaudit.thrift.configuration.LoggingAuditClientConfig;
import com.pinterest.singer.loggingaudit.thrift.configuration.AuditConfig;
import com.pinterest.singer.metrics.StatsPusher;
import com.pinterest.singer.thrift.configuration.DummyWriteConfig;
import com.pinterest.singer.thrift.configuration.FileNameMatchMode;
import com.pinterest.singer.thrift.configuration.HeartbeatWriterConfig;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.thrift.configuration.KafkaWriterConfig;
import com.pinterest.singer.thrift.configuration.KubeConfig;
import com.pinterest.singer.thrift.configuration.LogMonitorConfig;
import com.pinterest.singer.thrift.configuration.LogStreamProcessorConfig;
import com.pinterest.singer.thrift.configuration.LogStreamReaderConfig;
import com.pinterest.singer.thrift.configuration.LogStreamWriterConfig;
import com.pinterest.singer.thrift.configuration.PulsarProducerConfig;
import com.pinterest.singer.thrift.configuration.PulsarWriterConfig;
import com.pinterest.singer.thrift.configuration.ReaderType;
import com.pinterest.singer.thrift.configuration.RealpinObjectType;
import com.pinterest.singer.thrift.configuration.RealpinWriterConfig;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.thrift.configuration.SingerRestartConfig;
import com.pinterest.singer.thrift.configuration.TextLogMessageType;
import com.pinterest.singer.thrift.configuration.TextReaderConfig;
import com.pinterest.singer.thrift.configuration.ThriftReaderConfig;
import com.pinterest.singer.thrift.configuration.WriterType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.twitter.common.base.MorePreconditions;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.record.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.Iterator;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Arrays;

/**
 * Utility class for parsing singer related config file. The following is a
 * sample singer configuration file.
 *
 * # /etc/singer/singer.properties # singer.threadPoolSize = 20
 * singer.ostrichPort = 2047
 *
 * # Configuration for LogMonitor. singer.monitor.monitorIntervalInSecs = 10
 *
 * # Watcher interval secs singer.logConfigPollIntervalSecs = 10
 *
 * # stats pusher host ostrichPort singer.statsPusherHostPort = localhost:18126
 *
 * # whether to allow singer to restart itself # the number of errors that
 * singer needs to encounter before restarting # # Singer will restart itself
 * randomly in the time range, in UTC # singer.restart.onFailures=true
 * singer.restart.numberOfFailuresAllowed=100
 *
 * singer.restart.daily=true singer.restart.dailyRestartUtcTimeRangeBegin=02:30
 * singer.restart.dailyRestartUtcTimeRangeEnd=03:30
 *
 * # singer heartbeat configuration
 *
 * singer.heartbeat.intervalInSeconds = 60 singer.heartbeat.writer.writerType =
 * kafka singer.heartbeat.writer.kafka.topic = singer_heartbeat
 * singer.heartbeat.writer.kafka.producerConfig.metadata.broker.serverset =
 * /discovery/datakafka01/prod
 * singer.heartbeat.writer.kafka.producerConfig.acks = 1
 */

public class LogConfigUtils {

  private static final Logger LOG = LoggerFactory.getLogger(LogConfigUtils.class);
  public static final String DEFAULT_SERVERSET_DIR = "/var/serverset";
  private static final String DEFAULT_ACKS = "1";
  private static final String ACKS_ALL = "all";
  private static final long MaximumProcessingTimeSliceInMilliseconds = 864000000L;
  private static final ConcurrentMap<String, Set<String>> KAFKA_SERVER_SETS = Maps
      .newConcurrentMap();


  private LogConfigUtils() {
  }

  public static SingerConfig parseDirBasedSingerConfigHeader(String singerConfigFile) throws ConfigurationException {
    List<String> errors = new ArrayList<>();
    PropertiesConfiguration configHeader = new PropertiesConfiguration(singerConfigFile);
    AbstractConfiguration singerConfiguration = new SubsetConfiguration(configHeader,
        SingerConfigDef.SINGER_CONFIGURATION_PREFIX);

    SingerConfig result;
    try {
      result = parseCommonSingerConfigHeader(singerConfiguration);
    } catch (NoSuchElementException x) {
      result = new SingerConfig();
      errors
          .add(SingerConfigDef.MONITOR_INTERVAL_IN_SECS + " is required for Singer Configuration");
    }
    if (singerConfiguration.containsKey("logConfigPollIntervalSecs")) {
      result.setLogConfigPollIntervalSecs(singerConfiguration.getInt("logConfigPollIntervalSecs"));
    } else {
      errors.add("singer.setLogConfigPollIntervalSecs " + "is required for Singer Configuration");
    }

    try {
      result.singerRestartConfig = parseSingerRestartConfig(configHeader);
    } catch (ConfigurationException x) {
      errors.add(x.getMessage());
    }
    try {
      // if running in Kubernetes mode then set the pod directory to an empty string
      // if this line is removed and the configuration has a kubernetes log directory;
      // Singer will point to the wrong log path; with the kubernetes log path
      // pre-pended to the path
      if (result.isKubernetesEnabled()) {
        result.setKubeConfig(parseKubeConfig(configHeader));
      }
    } catch (ConfigurationException e) {
      errors.add(e.getMessage());
    }

    // check if heartbeat is enabled, don't auto enable if configuration is set
    // by default heartbeat is enabled in thrift so it will need to be explicitly
    // disabled
    if (result.isHeartbeatEnabled()) {
      try {
        AbstractConfiguration heartbeatConfig = new SubsetConfiguration(configHeader,
            "singer.heartbeat.");
        int intervalInSeconds = heartbeatConfig.getInt("intervalInSeconds");
        result.setHeartbeatIntervalInSeconds(intervalInSeconds);

        AbstractConfiguration heartbeatWriterConfig = new SubsetConfiguration(heartbeatConfig,
            "writer.");
        String writerType = heartbeatWriterConfig.getString("writerType");
        WriterType type = WriterType.valueOf(writerType.toUpperCase());
        HeartbeatWriterConfig writerConfig = new HeartbeatWriterConfig(type);
        switch (type) {
        case KAFKA08:
        case KAFKA:
          try {
            KafkaWriterConfig kafkaWriterConf = parseKafkaWriterConfig(
                new SubsetConfiguration(heartbeatWriterConfig, writerType + "."));
            writerConfig.setKafkaWriterConfig(kafkaWriterConf);
          } catch (ConfigurationException x) {
            errors.add(x.getMessage());
          }
          break;
        default:
          errors.add("Unsupported heartbeat writer type.");
        }
        result.setHeartbeatWriterConfig(writerConfig);
        result.setHeartbeatEnabled(true);
      } catch (java.util.NoSuchElementException e) {
        LOG.error("singer.heartbeat is not configured correctly.", e);
        result.setHeartbeatEnabled(false);
      }
    }

    if (result.isLoggingAuditEnabled()){
      AbstractConfiguration loggingAuditConf = new SubsetConfiguration(configHeader, "singer.loggingAudit.");
      try{
        LoggingAuditClientConfig loggingAuditClientConfig = ConfigUtils.parseLoggingAuditClientConfig(loggingAuditConf);
        if (loggingAuditClientConfig.getStage() != LoggingAuditStage.SINGER){
          LOG.warn("LoggingAudit stage should be SINGER.");
          loggingAuditClientConfig.setStage(LoggingAuditStage.SINGER);
        }
        result.setLoggingAuditClientConfig(loggingAuditClientConfig);
      } catch (ConfigurationException e){
        LOG.error("loggingAudit is not configured correctly.", e);
        result.setLoggingAuditEnabled(false);
      }
    }

    if (errors.size() > 0) {
      String errorMessage = Joiner.on('\n').join(errors);
      throw new ConfigurationException(errorMessage);
    }
    return result;
  }

  private static KubeConfig parseKubeConfig(PropertiesConfiguration configHeader) throws ConfigurationException {
    KubeConfig config = new KubeConfig();
    AbstractConfiguration subsetConfig = new SubsetConfiguration(configHeader,
        SingerConfigDef.SINGER_KUBE_CONFIG_PREFIX);
    if (subsetConfig.containsKey(SingerConfigDef.KUBE_POLL_FREQUENCY_SECONDS)) {
      config.setPollFrequencyInSeconds(
          subsetConfig.getInt(SingerConfigDef.KUBE_POLL_FREQUENCY_SECONDS));
    }

    if (subsetConfig.containsKey(SingerConfigDef.KUBE_POD_LOG_DIR)) {
      String logDirectory = subsetConfig.getString(SingerConfigDef.KUBE_POD_LOG_DIR);
      // normalize path before setting the property
      logDirectory = new File(logDirectory).toPath().normalize().toString() + "/";
      if (!logDirectory.isEmpty() && !logDirectory.endsWith("/")) {
        logDirectory += "/";
      }
      config.setPodLogDirectory(logDirectory);
    }

    if (subsetConfig.containsKey(SingerConfigDef.KUBE_DEFAULT_DELETION_TIMEOUT)) {
      config.setDefaultDeletionTimeoutInSeconds(
          subsetConfig.getInt(SingerConfigDef.KUBE_DEFAULT_DELETION_TIMEOUT));
    }
    return config;
  }

  /**
   * Singer can restart itself if # of failures exceeds threshold, and in daily
   * cadence. The following is singer restart related configuration:
   *
   * singer.restart.onFailures=true singer.restart.numberOfFailuresAllowed=100
   * singer.restart.daily=true singer.restart.dailyRestartUtcTimeRangeBegin=02:30
   * singer.restart.dailyRestartUtcTimeRangeEnd=03:30
   */
  private static SingerRestartConfig parseSingerRestartConfig(PropertiesConfiguration configHeader) throws ConfigurationException {
    SingerRestartConfig restartConfig = new SingerRestartConfig();
    AbstractConfiguration subsetConfig = new SubsetConfiguration(configHeader,
        SingerConfigDef.SINGER_RESTART_PREFIX);

    if (subsetConfig.containsKey(SingerConfigDef.ON_FAILURES)) {
      restartConfig.restartOnFailures = subsetConfig.getBoolean(SingerConfigDef.ON_FAILURES);
    }
    if (subsetConfig.containsKey(SingerConfigDef.NUMBER_OF_FAILURES_ALLOWED)) {
      restartConfig.numOfFailuesAllowed = subsetConfig
          .getInt(SingerConfigDef.NUMBER_OF_FAILURES_ALLOWED);
    }
    if (subsetConfig.containsKey(SingerConfigDef.DAILY_RESTART_FLAG)) {
      restartConfig.restartDaily = subsetConfig.getBoolean(SingerConfigDef.DAILY_RESTART_FLAG);
    }

    if (restartConfig.restartDaily) {
      if (!subsetConfig.containsKey(SingerConfigDef.DAILY_RESTART_TIME_BEGIN)
          || !subsetConfig.containsKey(SingerConfigDef.DAILY_RESTART_TIME_END)) {
        throw new ConfigurationException("Daily restart time range is not set correctly");
      }

      restartConfig.dailyRestartUtcTimeRangeBegin = subsetConfig
          .getString(SingerConfigDef.DAILY_RESTART_TIME_BEGIN);
      restartConfig.dailyRestartUtcTimeRangeEnd = subsetConfig
          .getString(SingerConfigDef.DAILY_RESTART_TIME_END);
      Date startTime = SingerUtils.convertToDate(restartConfig.dailyRestartUtcTimeRangeBegin);
      Date endTime = SingerUtils.convertToDate(restartConfig.dailyRestartUtcTimeRangeEnd);
      if (endTime.compareTo(startTime) <= 0) {
        throw new ConfigurationException("Daily restart end time is not later than start time");
      }
    }
    return restartConfig;
  }

  public static SingerConfig parseFileBasedSingerConfig(String singerConfigFile) throws ConfigurationException {
    AbstractConfiguration singerConfiguration = new SubsetConfiguration(
        new PropertiesConfiguration(singerConfigFile), SingerConfigDef.SINGER_CONFIGURATION_PREFIX);
    singerConfiguration.setThrowExceptionOnMissing(true);
    SingerConfig result = parseCommonSingerConfigHeader(singerConfiguration);
    Set<String> logs = Sets.newHashSet(singerConfiguration.getStringArray("logs"));
    for (String log : logs) {
      AbstractConfiguration logConfiguration = new SubsetConfiguration(singerConfiguration,
          log + ".");
      result.addToLogConfigs(LogConfigUtils.parseLogConfig(log, logConfiguration));
    }
    return result;
  }

  public static LogMonitorConfig parseLogMonitorConfig(AbstractConfiguration monitorConfiguration) {
    int monitorIntervalInSecs = monitorConfiguration.getInt("monitorIntervalInSecs");
    return new LogMonitorConfig(monitorIntervalInSecs);
  }

  public static SingerLogConfig parseLogConfigFromFile(File logConfigFile) throws ConfigurationException {
    AbstractConfiguration logConfig = new PropertiesConfiguration(logConfigFile);
    return parseLogConfig(parseLogNamespace(logConfigFile), logConfig);
  }

  public static SingerLogConfig[] parseLogStreamConfigFromFile(String logConfigFile) throws ConfigurationException {
    PropertiesConfiguration logConfig = new PropertiesConfiguration();
    logConfig.setDelimiterParsingDisabled(true);
    logConfig.load(new StringReader(logConfigFile));
    return parseLogStreamConfig(logConfig);
  }

  public static String parseLogNamespace(File logConfigFile) {
    return FilenameUtils.getBaseName(logConfigFile.getAbsolutePath());
  }

  public static SingerLogConfig parseLogConfig(String logName,
                                               AbstractConfiguration logConfiguration) throws ConfigurationException {
    logConfiguration.setThrowExceptionOnMissing(true);

    String local_dir = null;
    if (logConfiguration.containsKey("logDir")) {
      local_dir = logConfiguration.getString("logDir");
    } else if (logConfiguration.containsKey("local_dir")) {
      local_dir = logConfiguration.getString("local_dir");
    } else {
      throw new ConfigurationException("missing logDir/local_dir");
    }
    if (local_dir != null && local_dir.endsWith("/")) {
      local_dir = local_dir.substring(0, local_dir.length() - 1);
    }

    String logfile_regex;
    if (logConfiguration.containsKey("logStreamRegex")) {
      logfile_regex = logConfiguration.getString("logStreamRegex");
    } else if (logConfiguration.containsKey("logfile_regex")) {
      logfile_regex = logConfiguration.getString("logfile_regex");
    } else {
      throw new ConfigurationException("missing logStreamRegex/logfile_regex");
    }

    LogStreamProcessorConfig processorConfig = parseLogStreamProcessorConfig(
        new SubsetConfiguration(logConfiguration, "processor."));
    LogStreamReaderConfig readerConfig = parseLogStreamReaderConfig(
        new SubsetConfiguration(logConfiguration, "reader."));
    LogStreamWriterConfig writerConfig = parseLogStreamWriterConfig(
        new SubsetConfiguration(logConfiguration, "writer."));

    // initialize the optional fields
    logConfiguration.setThrowExceptionOnMissing(false);
    String logDecider = logConfiguration.getString("logDecider");
    SingerLogConfig config = new SingerLogConfig(logName, local_dir, logfile_regex, processorConfig,
        readerConfig, writerConfig);
    config.setLogDecider(logDecider);
    if (logConfiguration.containsKey("enableHeadersInjector")){
      boolean enableHeadersInjector = logConfiguration.getBoolean("enableHeadersInjector");
      config.setEnableHeadersInjector(enableHeadersInjector);
      if (enableHeadersInjector && logConfiguration.containsKey("headersInjectorClass")){
        config.setHeadersInjectorClass(logConfiguration.getString("headersInjectorClass"));
      }
    }

    FileNameMatchMode matchMode = FileNameMatchMode.PREFIX;
    String matchModeStr = logConfiguration.getString("logFileMatchMode");
    if (matchModeStr != null) {
      matchModeStr = matchModeStr.toLowerCase();
      if (matchModeStr.equals("exact")) {
        matchMode = FileNameMatchMode.EXACT;
      } else if (!matchModeStr.equals("prefix")) {
        throw new ConfigurationException("Invalid logFileNameMatchMode : " + matchModeStr);
      }
    }
    config.setFilenameMatchMode(matchMode);

    if (logConfiguration.containsKey(SingerConfigDef.LOG_RETENTION_SECONDS)) {
      config
          .setLogRetentionInSeconds(logConfiguration.getInt(SingerConfigDef.LOG_RETENTION_SECONDS));
    }

    if (logConfiguration.containsKey("enableLoggingAudit")){
      boolean enableLoggingAudit = logConfiguration.getBoolean("enableLoggingAudit");
      config.setEnableLoggingAudit(enableLoggingAudit);
      try {
        AuditConfig auditConfig = ConfigUtils.parseAuditConfig(new SubsetConfiguration(
            logConfiguration, "loggingaudit."));
        config.setAuditConfig(auditConfig);
      } catch(ConfigurationException e){
         LOG.error("TopicAuditConfig is not configured correctly for {}", logName, e);
         config.setEnableLoggingAudit(false);
      }
    }

    return config;
  }

  /**
   * Parse the given configuration. Unlike configs located in conf.d/, this config
   * contains the configuration for multiple kafka topics and so a list of
   * MercedTransporterConfigs are returned.
   */
  public static SingerLogConfig[] parseLogStreamConfig(AbstractConfiguration config) throws ConfigurationException {
    validateNewConfig(config);
    PropertiesConfiguration[] topicConfigs = getTopicConfigs(config);
    SingerLogConfig[] logConfigs = new SingerLogConfig[topicConfigs.length];

    for (int i = 0; i < topicConfigs.length; i++) {
      PropertiesConfiguration topicConfig = topicConfigs[i];
      String topicName = topicConfig.getString("name");
      logConfigs[i] = parseLogConfig(topicName, topicConfig);
    }
    return logConfigs;
  }

  /**
   * Parse the common singer configuration
   * @throws ConfigurationException 
   */
  public static SingerConfig parseCommonSingerConfigHeader(AbstractConfiguration singerConfiguration) throws ConfigurationException {
    SingerConfig singerConfig = new SingerConfig();
    if (singerConfiguration.containsKey("threadPoolSize")) {
      singerConfig.setThreadPoolSize(singerConfiguration.getInt("threadPoolSize"));
    }
    if (singerConfiguration.containsKey("ostrichPort")) {
      singerConfig.setOstrichPort(singerConfiguration.getInt("ostrichPort"));
    }
    if (singerConfiguration.containsKey("writerThreadPoolSize")) {
      int writerThreadPoolSize = singerConfiguration.getInt("writerThreadPoolSize");
      singerConfig.setWriterThreadPoolSize(writerThreadPoolSize);
    }
    singerConfig.setLogMonitorConfig(LogConfigUtils
        .parseLogMonitorConfig(new SubsetConfiguration(singerConfiguration, "monitor.")));
    if (singerConfiguration.containsKey("logFileRotationTimeInMillis")) {
      int logFileRotationTimeInMillis = singerConfiguration.getInt("logFileRotationTimeInMillis");
      singerConfig.setLogFileRotationTimeInMillis(logFileRotationTimeInMillis);
    }
    if (singerConfiguration.containsKey("logMonitorClass")) {
      String logMonitorClass = singerConfiguration.getString("logMonitorClass");
      singerConfig.setLogMonitorClass(logMonitorClass);
    }

    if (singerConfiguration.containsKey("kubernetesEnabled")) {
      singerConfig.setKubernetesEnabled(singerConfiguration.getBoolean("kubernetesEnabled"));
    }

    if (singerConfiguration.containsKey("heartbeatEnabled")) {
      singerConfig.setHeartbeatEnabled(singerConfiguration.getBoolean("heartbeatEnabled"));
    }
    
    if (singerConfiguration.containsKey("environmentProviderClass")) {
      String envProviderClass = singerConfiguration.getString("environmentProviderClass");
      singerConfig.setEnvironmentProviderClass(envProviderClass);
      // environmentProviderClass can be null therefore validation is only needed
      // if user has configured it
      try {
        Class<?> cls = Class.forName(envProviderClass);
        if (!EnvironmentProvider.class.isAssignableFrom(cls)) {
          throw new ConfigurationException("environmentProviderClass " + envProviderClass 
              + " doesn't extend " + EnvironmentProvider.class.getName());
        }
      } catch (ClassNotFoundException e) {
        throw new ConfigurationException("Couldn't find environmentProviderClass " + envProviderClass);
      }
    }

    String statsPusherHostPort = singerConfiguration.getString("statsPusherHostPort");
    if (statsPusherHostPort != null) {
      singerConfig.setStatsPusherHostPort(statsPusherHostPort);
      
      String statsPusherClass = singerConfiguration.getString("statsPusherClass");
      if (statsPusherClass != null) {
        singerConfig.setStatsPusherClass(statsPusherClass);
      }
      try {
        Class<?> cls = Class.forName(singerConfig.getStatsPusherClass());
        if (!StatsPusher.class.isAssignableFrom(cls)) {
          throw new ConfigurationException("statsPusherClass " + singerConfig.getStatsPusherClass() 
              + " doesn't extend " + StatsPusher.class.getName());
        }
      } catch (ClassNotFoundException e) {
        throw new ConfigurationException("Couldn't find statsPusherClass " + statsPusherClass);
      }
    }

    if (singerConfiguration.containsKey("loggingAuditEnabled")) {
      singerConfig.setLoggingAuditEnabled(singerConfiguration.getBoolean("loggingAuditEnabled"));
    }
    return singerConfig;
  }

  private static LogStreamWriterConfig parseLogStreamWriterConfig(AbstractConfiguration writerConfiguration) throws ConfigurationException {
    writerConfiguration.setThrowExceptionOnMissing(true);
    String writerTypeString = writerConfiguration.getString("type");
    WriterType type = WriterType.valueOf(writerTypeString.toUpperCase());
    LogStreamWriterConfig writerConfig = new LogStreamWriterConfig(type);
    switch (type) {
    case KAFKA08:
    case KAFKA:
      writerConfig.setKafkaWriterConfig(parseKafkaWriterConfig(
          new SubsetConfiguration(writerConfiguration, writerTypeString + ".")));
      return writerConfig;
    case REALPIN:
      writerConfig.setRealpinWriterConfig(parseRealpinWriterConfig(
          new SubsetConfiguration(writerConfiguration, writerTypeString + ".")));
      return writerConfig;
    case DUMMY:
      writerConfig.setDummyWriteConfig(parseDummyWriterConfig(
          new SubsetConfiguration(writerConfiguration, writerTypeString + ".")));
      return writerConfig;
    case PULSAR:
      writerConfig.setPulsarWriterConfig(parsePulsarWriterConfig(
          new SubsetConfiguration(writerConfiguration, writerTypeString + ".")));
      return writerConfig;
    default:
      throw new ConfigurationException("Unsupported log writer type.");
    }
  }

  private static PulsarWriterConfig parsePulsarWriterConfig(SubsetConfiguration writerConfiguration) throws ConfigurationException {
    PulsarWriterConfig config = new PulsarWriterConfig();
    config.setTopic(writerConfiguration.getString("topic"));
    config.setProducerConfig(
        parsePulsarProducerConfig(new SubsetConfiguration(writerConfiguration, "producerConfig.")));
    return config;
  }

  private static PulsarProducerConfig parsePulsarProducerConfig(SubsetConfiguration subsetConfiguration) throws ConfigurationException {
    PulsarProducerConfig pulsarProducerConfig = new PulsarProducerConfig();

    if (subsetConfiguration.containsKey(SingerConfigDef.COMPRESSION_TYPE)) {
      pulsarProducerConfig
          .setCompressionType(subsetConfiguration.getString(SingerConfigDef.COMPRESSION_TYPE));
    }
    if (subsetConfiguration.containsKey(SingerConfigDef.PARTITIONER_CLASS)) {
      pulsarProducerConfig.setPartitionerClass(subsetConfiguration.getString(SingerConfigDef.PARTITIONER_CLASS));
    }
    if (subsetConfiguration.containsKey(SingerConfigDef.PULSAR_SERVICE_URL)) {
      pulsarProducerConfig
          .setServiceUrl(subsetConfiguration.getString(SingerConfigDef.PULSAR_SERVICE_URL));
    } else {
      throw new ConfigurationException(
          "Missing Pulsar service URL in producer config:" + SingerConfigDef.PULSAR_SERVICE_URL);
    }
    pulsarProducerConfig.setPulsarClusterSignature(pulsarProducerConfig.getServiceUrl());
    return pulsarProducerConfig;
  }

  public static RealpinWriterConfig parseRealpinWriterConfig(AbstractConfiguration configuration) {
    configuration.setThrowExceptionOnMissing(true);
    String topic = configuration.getString(SingerConfigDef.TOPIC);
    String objectTypeString = configuration.getString(SingerConfigDef.REALPIN_OBJECT_TYPE);
    RealpinObjectType objectType = RealpinObjectType.valueOf(objectTypeString.toUpperCase());
    String serverSetPath = configuration.getString(SingerConfigDef.REALPIN_SERVERSET_PATH);

    RealpinWriterConfig writer = new RealpinWriterConfig(topic, objectType, serverSetPath);
    if (configuration.containsKey(SingerConfigDef.REALPIN_TIMEOUT_MS)) {
      writer.setTimeoutMs(configuration.getInt(SingerConfigDef.REALPIN_TIMEOUT_MS));
    }
    if (configuration.containsKey(SingerConfigDef.REALPIN_RETRIES)) {
      writer.setRetries(configuration.getInt(SingerConfigDef.REALPIN_RETRIES));
    }
    if (configuration.containsKey(SingerConfigDef.REALPIN_HOST_LIMIT)) {
      writer.setHostLimit(configuration.getInt(SingerConfigDef.REALPIN_HOST_LIMIT));
    }
    if (configuration.containsKey(SingerConfigDef.REALPIN_MAX_WAITERS)) {
      writer.setMaxWaiters(configuration.getInt(SingerConfigDef.REALPIN_MAX_WAITERS));
    }
    if (configuration.containsKey(SingerConfigDef.REALPIN_TTL)) {
      writer.setTtl(configuration.getInt(SingerConfigDef.REALPIN_TTL));
    }
    return writer;
  }

  private static KafkaWriterConfig parseKafkaWriterConfig(AbstractConfiguration kafkaWriterConfiguration) throws ConfigurationException {
    kafkaWriterConfiguration.setThrowExceptionOnMissing(true);
    String topic;
    try {
      topic = kafkaWriterConfiguration.getString(SingerConfigDef.TOPIC);
    } catch (Exception x) {
      throw new ConfigurationException("KafkaWriter topic is required for Singer Configuration");
    }
    KafkaProducerConfig producerConfig = parseProducerConfig(
        new SubsetConfiguration(kafkaWriterConfiguration, SingerConfigDef.PRODUCER_CONFIG_PREFIX));

    String auditTopic = null;

    boolean auditingEnabled = false;
    if (kafkaWriterConfiguration.containsKey(SingerConfigDef.AUDITING_ENABLED)) {
      auditingEnabled = kafkaWriterConfiguration.getBoolean(SingerConfigDef.AUDITING_ENABLED);

      if (auditingEnabled) {
        if (!kafkaWriterConfiguration.containsKey(SingerConfigDef.AUDIT_TOPIC)) {
          throw new ConfigurationException("Auditing enabled but missing audit topic");
        } else {
          auditTopic = kafkaWriterConfiguration.getString(SingerConfigDef.AUDIT_TOPIC);
        }
      }
    }

    boolean skipNoLeaderPartitions = false;
    if (kafkaWriterConfiguration.containsKey(SingerConfigDef.SKIP_NO_LEADER_PARTITIONS)) {
      skipNoLeaderPartitions = kafkaWriterConfiguration
          .getBoolean(SingerConfigDef.SKIP_NO_LEADER_PARTITIONS);
    }

    int writeTimeoutInSeconds = 60;
    if (kafkaWriterConfiguration.containsKey(SingerConfigDef.KAFKA_WRITE_TIMEOUT_IN_SECONDS)) {
      writeTimeoutInSeconds = kafkaWriterConfiguration
          .getInt(SingerConfigDef.KAFKA_WRITE_TIMEOUT_IN_SECONDS);
    }

    KafkaWriterConfig writerConfig = new KafkaWriterConfig(topic, producerConfig);
    writerConfig.setAuditTopic(auditTopic);
    writerConfig.setAuditingEnabled(auditingEnabled);
    writerConfig.setSkipNoLeaderPartitions(skipNoLeaderPartitions);
    writerConfig.setWriteTimeoutInSeconds(writeTimeoutInSeconds);
    return writerConfig;
  }

  private static DummyWriteConfig parseDummyWriterConfig(AbstractConfiguration configuration) {
    configuration.setThrowExceptionOnMissing(true);
    String topic = configuration.getString(SingerConfigDef.TOPIC);
    return new DummyWriteConfig(topic);
  }

  /**
   * Returns the file path on local disk corresponding to a ZooKeeper server set
   * path.
   *
   * E.g. /discovery/service/prod => /var/serverset/discovery.service.prod
   */
  public static String filePathFromZKPath(String serverSetZKPath) {
    MorePreconditions.checkNotBlank(serverSetZKPath);
    String filename = serverSetZKPath.replace('/', '.');
    filename = StringUtils.strip(filename, "."); // strip any leading or trailing dots.
    return new File(DEFAULT_SERVERSET_DIR, filename).getPath();
  }

  public static KafkaProducerConfig parseProducerConfig(AbstractConfiguration producerConfiguration) throws ConfigurationException {
    producerConfiguration.setThrowExceptionOnMissing(false);
    String partitionClass = producerConfiguration.getString(SingerConfigDef.PARTITIONER_CLASS);

    Set<String> brokerSet = Sets
        .newHashSet(producerConfiguration.getStringArray(SingerConfigDef.BOOTSTRAP_SERVERS));

    String serverSetFilePath = null;
    if (producerConfiguration.containsKey(SingerConfigDef.BOOTSTRAP_SERVERS_FILE)) {
      serverSetFilePath = producerConfiguration.getString(SingerConfigDef.BOOTSTRAP_SERVERS_FILE);
    } else if (producerConfiguration.containsKey(SingerConfigDef.BROKER_SERVERSET_DEPRECATED)) {
      String serversetZkPath = producerConfiguration
          .getString(SingerConfigDef.BROKER_SERVERSET_DEPRECATED);
      serverSetFilePath = filePathFromZKPath(serversetZkPath);
    }

    // Broker list will take precedence over broker serverset if both are set.
    if (brokerSet.isEmpty() && Strings.isNullOrEmpty(serverSetFilePath)) {
      throw new ConfigurationException(
          "metadata.broker.list or metadata.broker.serverset needs to be set.");
    }

    if (brokerSet.isEmpty()) {
      if (!KAFKA_SERVER_SETS.containsKey(serverSetFilePath)) {
        try {
          ConfigFileServerSet serverSet = new ConfigFileServerSet(serverSetFilePath);
          final String monitoredServersetFilePath = serverSetFilePath;
          // ConfigFileServerSet.monitor() guarantee that initial load will be done before
          // return.
          serverSet
              .monitor(new BrokerSetChangeListener(monitoredServersetFilePath, KAFKA_SERVER_SETS));
        } catch (Exception e) {
          throw new ConfigurationException("Cannot get broker list from serverset.", e);
        }
      }
      LOG.debug("Initial loading kafka broker serverset OK.");
      brokerSet = KAFKA_SERVER_SETS.get(serverSetFilePath);
    } else if (!Strings.isNullOrEmpty(serverSetFilePath)) {
      LOG.warn("Ignoring metadata.broker.serverset when metadata.broker.list is configured.");
    }

    producerConfiguration.setThrowExceptionOnMissing(true);
    String acks = producerConfiguration.containsKey(SingerConfigDef.ACKS)
        ? producerConfiguration.getString(SingerConfigDef.ACKS)
        : producerConfiguration.getString(SingerConfigDef.REQUEST_REQUIRED_ACKS, DEFAULT_ACKS);
    acks = acks.toLowerCase();

    if (!acks.equals(ACKS_ALL) && Math.abs(Integer.parseInt(acks)) > Integer.valueOf(DEFAULT_ACKS)) {
      throw new ConfigurationException(
          "Invalid ack configuration, " + SingerConfigDef.ACKS + "configuration value can only be 'all', -1, 0, 1");
    }

    // initialize producer config with the server set path as the cluster signature
    KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig(serverSetFilePath,
        Lists.newArrayList(brokerSet), acks);

    if (partitionClass != null) {
      // check if this config is a valid partitioner class 
      try {
        Class.forName(partitionClass);
      } catch (ClassNotFoundException e) {
        throw new ConfigurationException(
            "Unknown partitioner class: " + partitionClass);
      }
      kafkaProducerConfig.setPartitionerClass(partitionClass);
    } else {
      kafkaProducerConfig.setPartitionerClass(SingerConfigDef.DEFAULT_PARTITIONER);
    }

    if (producerConfiguration.containsKey(SingerConfigDef.COMPRESSION_TYPE)) {
      String compressionType = producerConfiguration.getString(SingerConfigDef.COMPRESSION_TYPE);
      if (compressionType != null && !compressionType.isEmpty()) {
        try {
          CompressionType.forName(compressionType);
        }catch(Exception e) {
          throw new ConfigurationException("Unknown compression type: " + compressionType);
        }
        kafkaProducerConfig.setCompressionType(compressionType);
      }
    }

    if (producerConfiguration.containsKey(ProducerConfig.MAX_REQUEST_SIZE_CONFIG)) {
      int maxRequestSize = producerConfiguration.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
      kafkaProducerConfig.setMaxRequestSize(maxRequestSize);
    }

    if (producerConfiguration.containsKey(SingerConfigDef.SSL_ENABLED_CONFIG)) {
      boolean enabled = producerConfiguration.getBoolean(SingerConfigDef.SSL_ENABLED_CONFIG);
      kafkaProducerConfig.setSslEnabled(enabled);
      if (enabled) {
        List<String> brokers = kafkaProducerConfig.getBrokerLists();
        List<String> updated = new ArrayList<>();
        for (int i = 0; i < brokers.size(); i++) {
          String broker = brokers.get(i);
          String[] ipports = broker.split(":");
          try {
            InetAddress addr = InetAddress.getByName(ipports[0]);
            String host = addr.getHostName();
            String newBrokerStr = host + ":9093";
            updated.add(newBrokerStr);
          } catch (UnknownHostException e) {
            LOG.error("Unknown host: {}", ipports[0]);
          }
        }
        kafkaProducerConfig.setBrokerLists(updated);

        Iterator<String> sslKeysIterator = producerConfiguration.getKeys("ssl");
        kafkaProducerConfig.setSslSettings(new HashMap<>());
        while (sslKeysIterator.hasNext()) {
          String key = sslKeysIterator.next();
          String value = key.equals(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG)
              ? String.join(",", producerConfiguration.getStringArray(key))
              : producerConfiguration.getString(key);
          kafkaProducerConfig.getSslSettings().put(key, value);
        }
      }
    }

    if (producerConfiguration.containsKey(SingerConfigDef.TRANSACTION_ENABLED_CONFIG)) {
      kafkaProducerConfig.setTransactionEnabled(true);
    }
    if (producerConfiguration.containsKey(SingerConfigDef.TRANSACTION_TIMEOUT_MS_CONFIG)) {
      int timeoutMs = producerConfiguration.getInt(SingerConfigDef.TRANSACTION_TIMEOUT_MS_CONFIG);
      kafkaProducerConfig.setTransactionTimeoutMs(timeoutMs);
    }
    if (producerConfiguration.containsKey(SingerConfigDef.RETRIES_CONFIG)) {
      int retries = producerConfiguration.getInt(SingerConfigDef.RETRIES_CONFIG);
      kafkaProducerConfig.setRetries(retries);
    }
    if (producerConfiguration.containsKey(SingerConfigDef.PRODUCER_BUFFER_MEMORY)) {
      int bufferMemory = producerConfiguration.getInt(SingerConfigDef.PRODUCER_BUFFER_MEMORY);
      kafkaProducerConfig.setBufferMemory(bufferMemory);
    }
    return kafkaProducerConfig;
  }

  private static LogStreamReaderConfig parseLogStreamReaderConfig(AbstractConfiguration readerConfiguration) throws ConfigurationException {
    readerConfiguration.setThrowExceptionOnMissing(true);
    String readerTypeString = readerConfiguration.getString("type");
    ReaderType type = ReaderType.valueOf(readerTypeString.toUpperCase());

    LogStreamReaderConfig readerConfig = new LogStreamReaderConfig(type);
    if (type.equals(ReaderType.THRIFT)) {
      ThriftReaderConfig thriftReaderConfig = parseThriftReaderConfig(
          new SubsetConfiguration(readerConfiguration, readerTypeString + "."));
      readerConfig.setThriftReaderConfig(thriftReaderConfig);
    } else if (type.equals(ReaderType.TEXT)) {
      TextReaderConfig textReaderConfig = parseTextReaderConfig(
          new SubsetConfiguration(readerConfiguration, readerTypeString + "."));
      readerConfig.setTextReaderConfig(textReaderConfig);
    }

    return readerConfig;
  }

  private static ThriftReaderConfig parseThriftReaderConfig(AbstractConfiguration thriftReaderConfiguration) {
    thriftReaderConfiguration.setThrowExceptionOnMissing(true);
    int readerBufferSize = thriftReaderConfiguration.getInt("readerBufferSize");
    int maxMessageSize = thriftReaderConfiguration.getInt("maxMessageSize");
    return new ThriftReaderConfig(readerBufferSize, maxMessageSize);
  }

  private static TextReaderConfig parseTextReaderConfig(AbstractConfiguration textReaderConfiguration) throws ConfigurationException {
    textReaderConfiguration.setThrowExceptionOnMissing(true);
    int readerBufferSize = textReaderConfiguration.getInt("readerBufferSize");
    Preconditions.checkArgument(readerBufferSize > 0, "Invalid readerBufferSize");
    int maxMessageSize = textReaderConfiguration.getInt("maxMessageSize");
    Preconditions.checkArgument(maxMessageSize > 0, "Invalid maxMessageSize");
    int numMessagesPerLogMessage = textReaderConfiguration.getInt("numMessagesPerLogMessage");
    Preconditions.checkArgument(numMessagesPerLogMessage > 0, "Invalid numMessagesPerLogMessage");
    String messageStartRegex = textReaderConfiguration.getString("messageStartRegex");
    // Verify the messageStartRegex is valid.
    try {
      Pattern.compile(messageStartRegex);
    } catch (PatternSyntaxException ex) {
      throw new ConfigurationException("Bad messageStartRegex", ex);
    }
    textReaderConfiguration.setThrowExceptionOnMissing(false);

    TextReaderConfig config = new TextReaderConfig(readerBufferSize, maxMessageSize,
        numMessagesPerLogMessage, messageStartRegex);
    if (textReaderConfiguration.containsKey("logMessageType")) {
      String logMessageType = textReaderConfiguration.getString("logMessageType");
      if (!logMessageType.isEmpty()) {
        TextLogMessageType type = TextLogMessageType.valueOf(logMessageType.toUpperCase());
        config.setTextLogMessageType(type);
      }
    }

    config.setPrependTimestamp(false);
    if (textReaderConfiguration.containsKey("prependTimestamp")) {
      String prependTimestampStr = textReaderConfiguration.getString("prependTimestamp");
      if (!prependTimestampStr.isEmpty()) {
        boolean value = Boolean.parseBoolean(prependTimestampStr);
        config.setPrependTimestamp(value);
      }
    }

    config.setPrependHostname(false);
    if (textReaderConfiguration.containsKey("prependHostname")) {
      String prependHostnameStr = textReaderConfiguration.getString("prependHostname");
      if (!prependHostnameStr.isEmpty()) {
        boolean value = Boolean.parseBoolean(prependHostnameStr);
        config.setPrependHostname(value);
      }
    }

    if (textReaderConfiguration.containsKey("prependFieldDelimiter")) {
      String str = textReaderConfiguration.getString("prependFieldDelimiter");
      // remove the quotes (can be single quote or double quotes) around the delimiter
      // string
      char firstChar = str.charAt(0);
      if (firstChar == '\'' || firstChar == '"' || firstChar == '`') {
        str = str.substring(1, str.length());
      }
      char lastChar = str.charAt(str.length() - 1);
      if (lastChar == '\'' || lastChar == '"' || lastChar == '`') {
        str = str.substring(0, str.length() - 1);
      }

      if (!str.isEmpty()) {
        config.setPrependFieldDelimiter(str);
      }
    }
    return config;
  }

  private static LogStreamProcessorConfig parseLogStreamProcessorConfig(AbstractConfiguration processorConfiguration) {
    processorConfiguration.setThrowExceptionOnMissing(true);
    long minIntervalInMillis;
    if (processorConfiguration.containsKey(SingerConfigDef.PROCESS_INTERVAL_MILLIS)) {
      minIntervalInMillis = processorConfiguration.getLong(SingerConfigDef.PROCESS_INTERVAL_MILLIS);
    } else {
      minIntervalInMillis = processorConfiguration.getLong(SingerConfigDef.PROCESS_INTERVAL_SECS)
          * 1000L;
    }

    int batchSize = processorConfiguration.getInt("batchSize");
    processorConfiguration.setThrowExceptionOnMissing(false);
    // Default to processingIntervalInSecondsMin.
    long maxIntervalInMillis = minIntervalInMillis;

    if (processorConfiguration.containsKey(SingerConfigDef.PROCESS_INTERVAL_MILLIS_MAX)) {
      maxIntervalInMillis = processorConfiguration
          .getLong(SingerConfigDef.PROCESS_INTERVAL_MILLIS_MAX);
    } else if (processorConfiguration.containsKey(SingerConfigDef.PROCESS_INTERVAL_SECS_MAX)) {
      maxIntervalInMillis = processorConfiguration
          .getLong(SingerConfigDef.PROCESS_INTERVAL_SECS_MAX) * 1000L;
    }

    long processingTimeSliceInMilliseconds = MaximumProcessingTimeSliceInMilliseconds;
    if (processorConfiguration.containsKey(SingerConfigDef.PROCESS_TIME_SLICE_MILLIS)) {
      String s = processorConfiguration.getString(SingerConfigDef.PROCESS_TIME_SLICE_MILLIS);
      if (!s.isEmpty()) {
        processingTimeSliceInMilliseconds = Long.parseLong(s);
      }
    } else if (processorConfiguration.containsKey(SingerConfigDef.PROCESS_TIME_SLICE_SECS)) {
      String s = processorConfiguration.getString(SingerConfigDef.PROCESS_TIME_SLICE_SECS);
      if (!s.isEmpty()) {
        processingTimeSliceInMilliseconds = Long.parseLong(s) * 1000L;
      }
    }

    LogStreamProcessorConfig config;
    config = new LogStreamProcessorConfig(minIntervalInMillis, maxIntervalInMillis, batchSize);
    config.setProcessingTimeSliceInMilliseconds(processingTimeSliceInMilliseconds);
    return config;
  }

  /**
   * Converts a Configuration object c to a PropertiesConfiguration by copying all
   * properties in c into an empty PropertiesConfiguration.
   *
   * @return A new PropertiesConfiguration object with all properties from c
   */
  private static PropertiesConfiguration toPropertiesConfiguration(Configuration c) {
    PropertiesConfiguration p = new PropertiesConfiguration();
    p.setDelimiterParsingDisabled(true);

    for (Iterator<String> it = c.getKeys(); it.hasNext();) {
      String key = it.next();
      String val = c.getString(key);
      p.setProperty(key, val);
    }
    return p;
  }

  /**
   * @param config - the properties configuration for the entire
   *               datapipelines.properties
   * @return an array of PropertiesConfiguration objects for each topic in
   *         datapipelines.properties
   */
  public static PropertiesConfiguration[] getTopicConfigs(Configuration config) {
    String topicNamesString = config.getString(SingerConfigDef.TOPIC_NAMES);
    if (topicNamesString.isEmpty()) {
      return new PropertiesConfiguration[0];
    }

    String[] topicNames = topicNamesString.split("[,\\s]+");
    PropertiesConfiguration[] topicConfigs = new PropertiesConfiguration[topicNames.length];

    for (int i = 0; i < topicNames.length; i++) {
      topicConfigs[i] = getTopicConfig(config, topicNames[i]);
    }
    return topicConfigs;
  }

  /**
   * get the PropertiesConfiguration for a specific topic from the file
   * datapipelines.properties
   *
   * @param config    - the properties configuration for the entire
   *                  datapipelines.properties
   * @param topicName - the name of the desired topic
   */
  public static PropertiesConfiguration getTopicConfig(Configuration config, String topicName) {
    // get default values, then copy the user-specified values into the default
    // values
    // user values will overwrite any default values.

    // get default settings
    PropertiesConfiguration topicConfig = toPropertiesConfiguration(
        config.subset("singer.default"));

    // add topic-specific default values
    topicConfig.setProperty("logfile_regex", topicName + "_(\\\\d+).log");
    topicConfig.setProperty("writer.kafka.topic", topicName);

    // get user values
    PropertiesConfiguration topicConfigOverrides = toPropertiesConfiguration(
        config.subset(topicName));

    // copy user settings into default values
    ConfigurationUtils.copy(topicConfigOverrides, topicConfig);
    return topicConfig;
  }

  /**
   * validates the datapipelines.properties configuration
   *
   * @throws ConfigurationException if any topics are in the topic_names
   *                                declaration at the top of the config, but do
   *                                not have any settings for it or vice-versa
   */
  public static void validateNewConfig(Configuration config) throws ConfigurationException {
    // get topic names from declaration in config
    String topicNamesString = config.getString(SingerConfigDef.TOPIC_NAMES);
    String[] topicNames = topicNamesString.isEmpty() ? new String[0]
        : topicNamesString.split("[,\\s]+");
    HashSet<String> topicsFromDeclaration = new HashSet<>(Arrays.asList(topicNames));

    // get topic names referenced by any setting
    HashSet<String> topicsFromSettings = new HashSet<>();
    for (Iterator<String> it = config.getKeys(); it.hasNext();) {
      // setting keys are in the form `topic_name.<a>.<b>`
      String settingKey = it.next();
      String topicName = settingKey.split("\\.")[0];

      // these are not settings
      if (topicName.equals("topic_names") || topicName.equals("merced")
          || topicName.equals("singer") || topicName.isEmpty()) {
        continue;
      }
      topicsFromSettings.add(topicName);
    }

    // determine which topics are in the declaration, but not in settings or
    // vice-versa. this is done with an XOR
    HashSet<String> errorTopics = new HashSet<>();
    errorTopics.addAll(topicsFromDeclaration);
    errorTopics.addAll(topicsFromSettings);
    topicsFromDeclaration.retainAll(topicsFromSettings);
    errorTopics.removeAll(topicsFromDeclaration);

    if (!errorTopics.isEmpty()) {
      throw new ConfigurationException(
          String.format("Following topics are not properly defined in %s: %s",
              DirectorySingerConfigurator.DATAPIPELINES_CONFIG, String.join(", ", errorTopics)));
    }
  }
}
