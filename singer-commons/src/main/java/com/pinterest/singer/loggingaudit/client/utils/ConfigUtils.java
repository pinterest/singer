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
package com.pinterest.singer.loggingaudit.client.utils;

import com.pinterest.singer.config.ConfigFileServerSet;
import com.pinterest.singer.loggingaudit.client.common.LoggingAuditClientConfigDef;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditStage;
import com.pinterest.singer.loggingaudit.thrift.configuration.LoggingAuditEventSenderConfig;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.loggingaudit.thrift.configuration.KafkaSenderConfig;
import com.pinterest.singer.loggingaudit.thrift.configuration.LoggingAuditClientConfig;
import com.pinterest.singer.loggingaudit.thrift.configuration.SenderType;
import com.pinterest.singer.loggingaudit.thrift.configuration.AuditConfig;
import com.pinterest.singer.utils.BrokerSetChangeListener;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.twitter.common.base.MorePreconditions;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.record.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;


public class ConfigUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigUtils.class);
  public static final String DEFAULT_SERVERSET_DIR = "/var/serverset";
  private static final ConcurrentMap<String, Set<String>> KAFKA_SERVER_SETS = Maps.newConcurrentMap();

  public static LoggingAuditClientConfig createLoggingAuditClientConfigFromKVs(
      Map<String, String> properties) throws ConfigurationException {
    LoggingAuditClientConfig loggingAuditClientConfig = new LoggingAuditClientConfig();
    if (!properties.containsKey(LoggingAuditClientConfigDef.STAGE)) {
      throw new ConfigurationException("STAGE is not properly set!");
    } else {
      loggingAuditClientConfig.setStage(LoggingAuditStage
          .valueOf(properties.get(LoggingAuditClientConfigDef.STAGE).toUpperCase()));
    }
    if (properties.containsKey(LoggingAuditClientConfigDef.DEFAULT_ENABLE_AUDIT_FOR_ALL_TOPICS)) {
      loggingAuditClientConfig.setEnableAuditForAllTopicsByDefault(Boolean.valueOf(
          properties.get(LoggingAuditClientConfigDef.DEFAULT_ENABLE_AUDIT_FOR_ALL_TOPICS)));
    }

    if (properties.containsKey(LoggingAuditClientConfigDef.QUEUE_SIZE)) {
      loggingAuditClientConfig
          .setQueueSize(Integer.valueOf(properties.get(LoggingAuditClientConfigDef.QUEUE_SIZE)));
    }

    if (properties.containsKey(LoggingAuditClientConfigDef.ENQUEUE_WAIT_IN_MILLISECONDS)) {
      loggingAuditClientConfig.setEnqueueWaitInMilliseconds(Integer
          .valueOf(properties.get(LoggingAuditClientConfigDef.ENQUEUE_WAIT_IN_MILLISECONDS)));
    }

    // parse kafka producer config
    if (!properties.containsKey(LoggingAuditClientConfigDef.BOOTSTRAP_SERVERS)) {
      throw new ConfigurationException("kafka bootstrap servers are not properly set!");
    }

    Set<String> brokerSet = Sets.newHashSet(properties.get(
        LoggingAuditClientConfigDef.BOOTSTRAP_SERVERS).split(","));
    String acks = properties.containsKey(LoggingAuditClientConfigDef.ACKS) ? properties.get(
        LoggingAuditClientConfigDef.ACKS) : LoggingAuditClientConfigDef.DEFAULT_ACKS;
    acks = acks.toLowerCase();
    KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig("",
        Lists.newArrayList(brokerSet), acks);

    KafkaSenderConfig kafkaSenderConfig = new KafkaSenderConfig();
    if (properties.containsKey(LoggingAuditClientConfigDef.KAFKA_TOPIC)) {
      kafkaSenderConfig.setTopic(properties.get(LoggingAuditClientConfigDef.KAFKA_TOPIC));
    }
    kafkaSenderConfig.setKafkaProducerConfig(kafkaProducerConfig);

    LoggingAuditEventSenderConfig senderConfig = new LoggingAuditEventSenderConfig();
    senderConfig.setKafkaSenderConfig(kafkaSenderConfig);

    loggingAuditClientConfig.setSenderConfig(senderConfig);
    return loggingAuditClientConfig;
  }

  public static LoggingAuditClientConfig parseFileBasedLoggingAuditClientConfig(
      String loggingAuditClientConfigFile) throws ConfigurationException {
    return parseFileBasedLoggingAuditClientConfig(loggingAuditClientConfigFile, "");
  }

  public static LoggingAuditClientConfig parseFileBasedLoggingAuditClientConfig(
      String loggingAuditClientConfigFile, String prefix) throws ConfigurationException {
    PropertiesConfiguration conf = new PropertiesConfiguration(loggingAuditClientConfigFile);
    return parseLoggingAuditClientConfig(conf, prefix);
  }


  public static LoggingAuditClientConfig parseLoggingAuditClientConfig(
      AbstractConfiguration conf, String prefix) throws ConfigurationException {
    if (prefix != null && prefix != ""){
      conf = new SubsetConfiguration(conf, prefix);
    }
    LoggingAuditClientConfig loggingAuditClientConfig = parseCommonConfig(conf);
    loggingAuditClientConfig.setAuditConfigs(parseAllAuditConfigs(
        new SubsetConfiguration(conf, LoggingAuditClientConfigDef.AUDITED_TOPICS_PREFIX)));
    loggingAuditClientConfig.setSenderConfig(parseSenderConfig(
        new SubsetConfiguration(conf, LoggingAuditClientConfigDef.SENDER_PREFIX)));
    return loggingAuditClientConfig;
  }


  public static LoggingAuditClientConfig parseCommonConfig(AbstractConfiguration conf)
      throws ConfigurationException {
    LoggingAuditClientConfig loggingAuditClientConfig = new LoggingAuditClientConfig();
    try {
      loggingAuditClientConfig.setStage(LoggingAuditStage
          .valueOf(conf.getString(LoggingAuditClientConfigDef.STAGE).toUpperCase()));
    } catch (Exception e) {
      throw new ConfigurationException("STAGE is not properly set!");
    }

    if (conf.containsKey(LoggingAuditClientConfigDef.DEFAULT_ENABLE_AUDIT_FOR_ALL_TOPICS)) {
      loggingAuditClientConfig.setEnableAuditForAllTopicsByDefault(
          conf.getBoolean(LoggingAuditClientConfigDef.DEFAULT_ENABLE_AUDIT_FOR_ALL_TOPICS));
    }

    if (conf.containsKey(LoggingAuditClientConfigDef.QUEUE_SIZE)) {
      loggingAuditClientConfig.setQueueSize(conf.getInt(LoggingAuditClientConfigDef.QUEUE_SIZE));
    }
    if (conf.containsKey(LoggingAuditClientConfigDef.ENQUEUE_WAIT_IN_MILLISECONDS)) {
      loggingAuditClientConfig.setEnqueueWaitInMilliseconds(
          conf.getInt(LoggingAuditClientConfigDef.ENQUEUE_WAIT_IN_MILLISECONDS));
    }
    return loggingAuditClientConfig;
  }


  public static Map<String, AuditConfig> parseAllAuditConfigs(AbstractConfiguration conf) {
    Map<String, AuditConfig> auditConfigs = new HashMap<>();
    if (conf != null && conf.containsKey(LoggingAuditClientConfigDef.AUDITED_TOPIC_NAMES)) {
      for (String name : conf.getStringArray(LoggingAuditClientConfigDef.AUDITED_TOPIC_NAMES)) {
        try {
          auditConfigs.put(name, parseAuditConfig(new SubsetConfiguration(conf, name + ".")));
        } catch (ConfigurationException e) {
          LOG.error("Can't parse TopicAuditConfig for {}", name);
        }
      }
    }
    return auditConfigs;
  }

  public static AuditConfig parseAuditConfig(AbstractConfiguration conf)
      throws ConfigurationException {
    AuditConfig topicAuditConfig = new AuditConfig();
    try {
      if (conf.containsKey(LoggingAuditClientConfigDef.SAMPLING_RATE)) {
        topicAuditConfig
            .setSamplingRate(conf.getDouble(LoggingAuditClientConfigDef.SAMPLING_RATE));
      }
      if (conf.containsKey(LoggingAuditClientConfigDef.START_AT_CURRENT_STAGE)) {
        topicAuditConfig.setStartAtCurrentStage(
            conf.getBoolean(LoggingAuditClientConfigDef.START_AT_CURRENT_STAGE));
      }
      if (conf.containsKey(LoggingAuditClientConfigDef.STOP_AT_CURRENT_STAGE)) {
        topicAuditConfig
            .setStopAtCurrentStage(
                conf.getBoolean(LoggingAuditClientConfigDef.STOP_AT_CURRENT_STAGE));
      }
      if (conf.containsKey(LoggingAuditClientConfigDef.SKIP_CORRUPTED_MESSAGE_AT_CURRENT_STAGE)) {
        topicAuditConfig
                .setSkipCorruptedMessageAtCurrentStage(
                        conf.getBoolean(LoggingAuditClientConfigDef.SKIP_CORRUPTED_MESSAGE_AT_CURRENT_STAGE));
      }
      return topicAuditConfig;
    } catch (Exception e) {
      throw new ConfigurationException("Can't create TopicAuditConfig from configuration.", e);
    }
  }

  public static AuditConfig createAuditConfigFromMap(Map<String, String> properties)
      throws ConfigurationException {
    AuditConfig topicAuditConfig = new AuditConfig();
    try {
      if (properties.containsKey(LoggingAuditClientConfigDef.SAMPLING_RATE)) {
        topicAuditConfig.setSamplingRate(
            Double.valueOf(properties.get(LoggingAuditClientConfigDef.SAMPLING_RATE)));
      }
      if (properties.containsKey(LoggingAuditClientConfigDef.START_AT_CURRENT_STAGE)) {
        topicAuditConfig.setStartAtCurrentStage(
            Boolean.valueOf(properties.get(LoggingAuditClientConfigDef.START_AT_CURRENT_STAGE)));
      }
      if (properties.containsKey(LoggingAuditClientConfigDef.STOP_AT_CURRENT_STAGE)) {
        topicAuditConfig.setStopAtCurrentStage(
            Boolean.valueOf(properties.get(LoggingAuditClientConfigDef.STOP_AT_CURRENT_STAGE)));
      }
      return topicAuditConfig;
    } catch (Exception e) {
      throw new ConfigurationException("Can't create TopicAuditConfig from k-v pairs.", e);
    }
  }

  public static LoggingAuditEventSenderConfig parseSenderConfig(AbstractConfiguration conf)
      throws ConfigurationException {
    try {
      LoggingAuditEventSenderConfig senderConfig = new LoggingAuditEventSenderConfig();
      SenderType senderType = SenderType.valueOf(conf.getString(
          LoggingAuditClientConfigDef.SENDER_TYPE).toUpperCase());
      if (!senderType.equals(SenderType.KAFKA)) {
        throw new ConfigurationException("Only Kafka Sender is supported now.");
      }
      senderConfig.setSenderType(senderType);
      senderConfig.setKafkaSenderConfig(parseKafkaSenderConfig(new SubsetConfiguration(conf,
          LoggingAuditClientConfigDef.KAFKA_SENDER_PREFIX)));
      return senderConfig;
    } catch (Exception e) {
      throw new ConfigurationException(
          "LoggingAuditEventSenderConfig can't be properly parsed due to " + e.getMessage());
    }
  }


  public static KafkaSenderConfig parseKafkaSenderConfig(AbstractConfiguration conf)
      throws ConfigurationException {
    KafkaSenderConfig kafkaSenderConfig = new KafkaSenderConfig();
    if (conf.containsKey(LoggingAuditClientConfigDef.KAFKA_TOPIC)) {
      kafkaSenderConfig.setTopic(conf.getString(LoggingAuditClientConfigDef.KAFKA_TOPIC));
    }
    if (conf.containsKey(LoggingAuditClientConfigDef.KAFKA_STOP_GRACE_PERIOD_IN_SECONDS)) {
      kafkaSenderConfig.setStopGracePeriodInSeconds(
          conf.getInt(LoggingAuditClientConfigDef.KAFKA_STOP_GRACE_PERIOD_IN_SECONDS));
    }
    kafkaSenderConfig.setKafkaProducerConfig(parseProducerConfig(
        new SubsetConfiguration(conf, LoggingAuditClientConfigDef.KAFKA_PRODUCER_CONFIG_PREFIX)));
    return kafkaSenderConfig;
  }

  public static KafkaProducerConfig parseProducerConfig(AbstractConfiguration producerConfiguration)
      throws ConfigurationException {
    producerConfiguration.setThrowExceptionOnMissing(true);
    Set<String> brokerSet = Sets.newHashSet(producerConfiguration.getStringArray(
        LoggingAuditClientConfigDef.BOOTSTRAP_SERVERS));

    String serverSetFilePath = null;
    if (producerConfiguration.containsKey(LoggingAuditClientConfigDef.BOOTSTRAP_SERVERS_FILE)) {
      serverSetFilePath = producerConfiguration.getString(
          LoggingAuditClientConfigDef.BOOTSTRAP_SERVERS_FILE);
    } else if (producerConfiguration.containsKey(
        LoggingAuditClientConfigDef.BROKER_SERVERSET_DEPRECATED)) {
      String serversetZkPath = producerConfiguration.getString(
          LoggingAuditClientConfigDef.BROKER_SERVERSET_DEPRECATED);
      serverSetFilePath = filePathFromZKPath(serversetZkPath);
    }

    // Broker list will take precedence over broker serverset if both are set.
    if (brokerSet.isEmpty() && Strings.isNullOrEmpty(serverSetFilePath)) {
      throw new ConfigurationException(
          "bootstrap.servers or bootstrap.servers.file or metadata.broker.serverset needs to be set.");
    }

    if (brokerSet.isEmpty()) {
      if (!KAFKA_SERVER_SETS.containsKey(serverSetFilePath)) {
        try {
          ConfigFileServerSet serverSet = new ConfigFileServerSet(serverSetFilePath);
          final String monitoredServersetFilePath = serverSetFilePath;
          // ConfigFileServerSet.monitor() guarantee that initial load will be done before
          // return.
          serverSet.monitor(new BrokerSetChangeListener(monitoredServersetFilePath, KAFKA_SERVER_SETS));
        } catch (Exception e) {
          throw new ConfigurationException("Cannot get broker list from serverset.", e);
        }
      }
      LOG.debug("Initial loading kafka broker serverset OK.");
      brokerSet = KAFKA_SERVER_SETS.get(serverSetFilePath);
    } else if (!Strings.isNullOrEmpty(serverSetFilePath)) {
      LOG.warn("Ignoring metadata.broker.serverset when metadata.broker.list is configured.");
    }

    String acks = producerConfiguration.containsKey(LoggingAuditClientConfigDef.ACKS) ?
                  producerConfiguration.getString(LoggingAuditClientConfigDef.ACKS) :
                  LoggingAuditClientConfigDef.DEFAULT_ACKS;
    acks = acks.toLowerCase();
    KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig(
        serverSetFilePath, Lists.newArrayList(brokerSet), acks);

    if (producerConfiguration.containsKey(LoggingAuditClientConfigDef.COMPRESSION_TYPE)) {
      String compressionType = producerConfiguration.getString(
          LoggingAuditClientConfigDef.COMPRESSION_TYPE);
      if (compressionType != null && !compressionType.isEmpty()) {
        try {
          CompressionType.forName(compressionType);
        } catch (Exception e) {
          throw new ConfigurationException("Unknown compression type: " + compressionType);
        }
        kafkaProducerConfig.setCompressionType(compressionType);
      }
    }

    if (producerConfiguration.containsKey(ProducerConfig.MAX_REQUEST_SIZE_CONFIG)) {
      int maxRequestSize = producerConfiguration.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
      kafkaProducerConfig.setMaxRequestSize(maxRequestSize);
    }

    if (producerConfiguration.containsKey(ProducerConfig.LINGER_MS_CONFIG)) {
      int lingerMs = producerConfiguration.getInt(ProducerConfig.LINGER_MS_CONFIG);
      kafkaProducerConfig.setLingerMs(lingerMs);
    }

    if (producerConfiguration.containsKey(LoggingAuditClientConfigDef.SSL_ENABLED_CONFIG)) {
      boolean enabled = producerConfiguration.getBoolean(
          LoggingAuditClientConfigDef.SSL_ENABLED_CONFIG);
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

        Iterator<String> sslKeysIterator = producerConfiguration.getKeys(
            LoggingAuditClientConfigDef.SECURE_KAFKA_PRODUCER_CONFIG_PREFIX);
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

    if (producerConfiguration.containsKey(LoggingAuditClientConfigDef.TRANSACTION_ENABLED_CONFIG)) {
      kafkaProducerConfig.setTransactionEnabled(true);
    }
    if (producerConfiguration.containsKey(
        LoggingAuditClientConfigDef.TRANSACTION_TIMEOUT_MS_CONFIG)) {
      int timeoutMs = producerConfiguration.getInt(
          LoggingAuditClientConfigDef.TRANSACTION_TIMEOUT_MS_CONFIG);
      kafkaProducerConfig.setTransactionTimeoutMs(timeoutMs);
    }
    if (producerConfiguration.containsKey(LoggingAuditClientConfigDef.RETRIES_CONFIG)) {
      int retries = producerConfiguration.getInt(LoggingAuditClientConfigDef.RETRIES_CONFIG);
      kafkaProducerConfig.setRetries(retries);
    }
    return kafkaProducerConfig;
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

}
