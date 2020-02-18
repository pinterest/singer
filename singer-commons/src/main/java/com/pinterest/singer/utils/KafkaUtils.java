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


import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import kafka.cluster.Broker;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import scala.Tuple2;
import scala.collection.Seq;


public class KafkaUtils {

  private static final String DEFAULT_NAME_PREFIX = "singer_";
  public static final int DEFAULT_PRODUCER_BUFFER_MEMORY = 1024;

  private static Map<String, ZkUtils> zkUtilsMap = new HashMap<>();

  public static KafkaProducer<byte[], byte[]> createKafkaProducer(KafkaProducerConfig config){
    return createKafkaProducer(config, DEFAULT_NAME_PREFIX);
  }

  public static KafkaProducer<byte[], byte[]> createKafkaProducer(KafkaProducerConfig config, String namePrefix) {
    String brokerList = Joiner.on(',').join(config.getBrokerLists());
    Properties properties = new Properties();
    // singer use namePrefix : "singer_"
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, namePrefix + CommonUtils.getHostName() + "_" + UUID.randomUUID());
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.getKeySerializerClass());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.getValueSerializerClass());
    if (config.getBufferMemory() >= DEFAULT_PRODUCER_BUFFER_MEMORY) {
      // make sure that there is at least some reasonable amount of memory buffer
      // if that's not the case use Kafka producer default
      properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getBufferMemory());
    }

    if (config.isTransactionEnabled()) {
      properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
      String transactionalId = namePrefix + CommonUtils.getHostName();
      properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
      properties.put(ProducerConfig.ACKS_CONFIG, "all");
      properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, config.getTransactionTimeoutMs());
    } else {
      properties.put(ProducerConfig.ACKS_CONFIG, String.valueOf(config.getAcks()));
    }
    if (config.isSetRetries()) {
      properties.put(ProducerConfig.RETRIES_CONFIG, config.getRetries());
    }

    if (config.isSetCompressionType()) {
      properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getCompressionType());
    }
    if (config.isSetMaxRequestSize()) {
      properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, config.getMaxRequestSize());
    }

    // ssl related kafka producer configuration
    if (config.isSslEnabled()) {
      List<String> missingConfigurations = new ArrayList<>();
      Map<String, String> sslSettings = config.getSslSettings();
      if (!sslSettings.containsKey(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG)) {
        missingConfigurations.add(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG);
      }
      if (!sslSettings.containsKey(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG)) {
        missingConfigurations.add(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
      }
      if (!sslSettings.containsKey(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG)) {
        missingConfigurations.add(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
      }
      if (!sslSettings.containsKey(SslConfigs.SSL_KEY_PASSWORD_CONFIG)) {
        missingConfigurations.add(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
      }
      if (!sslSettings.containsKey(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)) {
        missingConfigurations.add(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
      }
      if (!sslSettings.containsKey(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)) {
        missingConfigurations.add(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
      }
      if (!sslSettings.containsKey(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG)) {
        missingConfigurations.add(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
      }
      if (!sslSettings.containsKey(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG)) {
        missingConfigurations.add(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
      }
      if (!sslSettings.containsKey(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)) {
        missingConfigurations.add(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
      }
      if (!sslSettings.containsKey(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)) {
        missingConfigurations.add(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
      }
      if (!sslSettings.containsKey(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG)) {
        missingConfigurations.add(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG);
      }
      if (!missingConfigurations.isEmpty()) {
        String errorMessage = String.join(",", missingConfigurations);
        throw new ConfigException("Missing configuration : " + errorMessage);
      }

      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
      properties.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 30000L);
      properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
      for (Map.Entry<String, String> entry : sslSettings.entrySet()) {
        properties.put(entry.getKey(), entry.getValue());
      }
    }

    KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties);
    return producer;
  }


  public static ZkUtils getZkUtils(String zkUrl) {
    if (!zkUtilsMap.containsKey(zkUrl)) {
      Tuple2<ZkClient, ZkConnection> zkClientAndConnection = ZkUtils.createZkClientAndConnection(zkUrl, 300, 3000000);
      ZkUtils zkUtils = new ZkUtils(zkClientAndConnection._1(), zkClientAndConnection._2(), true);
      zkUtilsMap.put(zkUrl, zkUtils);
    }
    return zkUtilsMap.get(zkUrl);
  }

  public static String getBrokers(String zkUrl, SecurityProtocol securityProtocol) {
    ZkUtils zkUtils = getZkUtils(zkUrl);
    Seq<Broker> brokersSeq = zkUtils.getAllBrokersInCluster();
    Broker[] brokers = new Broker[brokersSeq.size()];
    brokersSeq.copyToArray(brokers);
    String brokersStr = Arrays.stream(brokers).map((b) -> {
      return b.brokerEndPoint(ListenerName.forSecurityProtocol(securityProtocol)).connectionString();
    }).reduce(null, (a, b) -> {
      return a == null ? b : a + "," + b;
    });
    return brokersStr;
  }
}
