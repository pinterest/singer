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
package com.pinterest.singer.tools;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;

import com.pinterest.singer.config.DirectorySingerConfigurator;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.thrift.configuration.KafkaWriterConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.thrift.configuration.WriterType;
import com.pinterest.singer.utils.LogConfigUtils;

/**
 * Tool for checking LogConfig file to be valid
 */
public class LogConfigCheckTool {

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    if (args.length < 1) {
      System.err.println("Must specify the configuration directory");
      System.exit(-1);
    }

    String configDirectory = args[0];
    File confDir = new File(configDirectory);

    if (!confDir.isDirectory()) {
      System.out.println("Supplied path is not a directory:" + configDirectory);
      System.exit(-1);
    }

    for (String logFile : confDir.list()) {
      if (!logFile.matches(DirectorySingerConfigurator.CONFIG_FILE_PATTERN)) {
        System.err.println("Invalid configuration file name:" + logFile);
        System.exit(-1);
      }
    }

    Map<String, Set<String>> topicMap = new HashMap<>();
    File[] listFiles = confDir.listFiles();
    Arrays.sort(listFiles);
    for (File file : listFiles) {
      try {
        SingerLogConfig slc = LogConfigUtils.parseLogConfigFromFile(file);
        KafkaWriterConfig kafkaWriterConfig = slc.getLogStreamWriterConfig().getKafkaWriterConfig();
        if ((slc.getLogStreamWriterConfig().getType() == WriterType.KAFKA
            || slc.getLogStreamWriterConfig().getType() == WriterType.KAFKA08)) {
          if (kafkaWriterConfig == null) {
            System.err
                .println("Kafka writer specified with missing Kafka config:" + file.getName());
            System.exit(-1);
          } else {
            KafkaProducerConfig producerConfig = kafkaWriterConfig.getProducerConfig();
            if (producerConfig.getBrokerLists().isEmpty()) {
              System.err
                  .println("No brokers in the kafka configuration specified:" + file.getName());
              System.exit(-1);
            }
            String broker = producerConfig.getBrokerLists().get(0);
            Set<String> topics = topicMap.get(broker);
            if (topics == null) {
              topics = new HashSet<>();
              topicMap.put(broker, topics);
            }
            topics.add(kafkaWriterConfig.getTopic() + " " + file.getName());
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        System.err.println("Bad configuration file:" + file.getName());
        System.exit(-1);
      }
    }
    for (Entry<String, Set<String>> entry : topicMap.entrySet()) {
      String bootstrapBroker = entry.getKey();
      Properties properties = new Properties();
      bootstrapBroker = bootstrapBroker.replace("9093", "9092");
      properties.put("bootstrap.servers", bootstrapBroker);
      AdminClient cl = AdminClient.create(properties);
      Set<String> topicSet = cl.listTopics().names().get();
      for (String topicFileMap : entry.getValue()) {
        String[] splits = topicFileMap.split(" ");
        String topic = splits[0];
        if (!topicSet.contains(topic)) {
          System.err.println("Topic:" + topic + " doesn't exist for file:" + splits[1]);
          cl.close();
          System.exit(-1);
        }
      }
      cl.close();
    }
  }

}