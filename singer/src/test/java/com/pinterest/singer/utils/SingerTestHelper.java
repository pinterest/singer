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

import com.pinterest.singer.client.ThriftLoggerFactory;
import com.pinterest.singer.common.SingerStatus;
import com.pinterest.singer.thrift.Event;

import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

public class SingerTestHelper {

  /**
   * Set up the singer config files.
   */
  public static void createSingerConfig(String configDir,
                                        String configConfDir,
                                        String dataDir,
                                        String logStreamRegex,
                                        int batchSize,
                                        String topic,
                                        String auditTopic,
                                        int heartbeatInterval,
                                        String heartbeatTopic) {
    BufferedWriter writer = null;

    // creat singer.properties file
    try {
      File confDir = new File(configDir);
      File conf = new File(configConfDir);
      confDir.mkdirs();
      conf.mkdirs();

      File singerConf = new File(configDir + "/singer.properties");
      writer = new BufferedWriter(new FileWriter(singerConf));
      writer.write("singer.threadPoolSize = 8\n");
      writer.write("singer.writerThreadPoolSize = 8\n");
      writer.write("singer.ostrichPort = 2057\n\n");
      writer.write("# Configuration for LogMonitor.\n");
      writer.write("singer.monitor.monitorIntervalInSecs = 10\n\n");
      writer.write("# Watcher interval secs\n");
      writer.write("singer.logConfigPollIntervalSecs = 10 \n\n");
      writer.write("# stats pusher host ostrichPort \n");
      writer.write("singer.statsPusherHostPort = localhost:18126 \n");

      writer.write("# singer heartbeat configuration \n");
      writer.write(
          "singer.heartbeat.intervalInSeconds = " + Integer.toString(heartbeatInterval) + "\n");
      writer.write("singer.heartbeat.writer.writerType = kafka\n");
      writer.write("singer.heartbeat.writer.kafka.topic = " + heartbeatTopic + "\n");
      writer.write("singer.heartbeat.writer.kafka.producerConfig.bootstrap.servers.file="
              + "/var/serverset/discovery.datakafka01.prod\n");
      writer.write("singer.heartbeat.writer.kafka.producerConfig.acks=1\n");
    } catch (Exception e) {
      e.printStackTrace();
      assert (false);
    } finally {
      try {
        writer.close();
      } catch (Exception e) {
        e.printStackTrace();
        assert (false);
      }
    }

    createSingerConfigLConfFile(configConfDir, "data.test.properties", dataDir, logStreamRegex,
        batchSize, topic, auditTopic);
  }


  public static void createSingerConfigLConfFile(String configConfDir,
                                                 String configConfFileName,
                                                 String dataDir,
                                                 String logStreamRegex,
                                                 int batchSize,
                                                 String topic,
                                                 String auditTopic) {
    BufferedWriter writer = null;
    try {
      File singerConf = new File(configConfDir + "/" + configConfFileName);
      writer = new BufferedWriter(new FileWriter(singerConf));

      String content =
          "# Log level configuration for data log. \n" +
              "logName = data_log \n" +
              "logDir = " + dataDir + "\n" +
              "logStreamRegex = " + logStreamRegex + "\n" +
              "\n" +
              "# Configuration for processor. \n" +
              "processor.batchSize = " + batchSize + " \n" +
              "processor.processingIntervalInSeconds = 1 \n" +
              "processor.processingIntervalInSecondsMax = 1 \n" +
              "\n" +
              "# Configuration for reader \n" +
              "reader.type = thrift \n" +
              "# 512k buffer \n" +
              "reader.thrift.readerBufferSize = 524288 \n" +
              "# maximum thrift message size: 512k \n" +
              "reader.thrift.maxMessageSize = 524288 \n" +
              "\n" +
              "# Configuration for writer \n" +
              "writer.type=kafka  \n" +
              "writer.kafka.topic=" + topic + "\n" +
              "writer.kafka.auditTopic=" + auditTopic + "\n" +
              "writer.kafka.auditingEnabled=true \n" +
              "writer.kafka.producerConfig.bootstrap.servers.file=/var/serverset/discovery.testkafka.prod\n" +
              "writer.kafka.producerConfig.acks=-1\n" +
              "writer.kafka.producerConfig.compression.codec=gzip\n" +
              "writer.kafka.producerConfig.max.request.size=1024000\n" +
              "writer.kafka.producerConfig.transactionEnabled=true\n" +
              "writer.kafka.producerConfig.transaction.timeout.ms=6000\n" +
              "writer.kafka.producerConfig.retries=5\n";
      writer.write(content);
    } catch (Exception e) {
      e.printStackTrace();
      assert (false);
    } finally {
      try {
        writer.close();
      } catch (Exception e) {
        e.printStackTrace();
        assert (false);
      }
    }
  }

  public static Process startSingerProcess(String singerBinaryDir, String singerConfigDir,
                                           Class<?> javaClass) {
    URL location = javaClass.getProtectionDomain().getCodeSource().getLocation();
    String path = location.getFile();

    int pos = path.lastIndexOf('/');
    path = path.substring(0, pos);
    pos = path.lastIndexOf('/');
    path = path.substring(0, pos);

    File srcDir = new File(path);
    File destDir = new File(singerBinaryDir);
    try {
      FileUtils.copyDirectory(srcDir, destDir);
    } catch (IOException e) {
      e.printStackTrace();
      assert (false);
    }

    String singerVersion = "unkown";
    try {
      Properties properties = new Properties();
      InputStream inputStream = SingerStatus.class.getResourceAsStream("/build.properties");
      properties.load(inputStream);
      singerVersion = properties.getProperty("version");
    } catch (IOException e) {
    }

    String singerCmd;
    String newConfigFlag = System.getProperty("useNewConfig") != null ? "-DuseNewConfig" : "";
    singerCmd = String.format("/usr/bin/java8 %s -server -Xmx500M -Xms500M -XX:NewSize=300M -verbosegc " +
        " -cp " + destDir + ":" + destDir + "/lib/*:" + destDir +
        "/singer-" + singerVersion + ".jar " +
        "-Dlog4j.configuration=" + destDir + "/classes/log4j.prod.properties -Dsinger.config.dir=" +
        singerConfigDir + " com.pinterest.singer.SingerMain", newConfigFlag);

    System.out.println(singerCmd);

    Process proc = null;
    try {
      proc = Runtime.getRuntime().exec(singerCmd);
      return proc;
    } catch (Exception e) {
      e.printStackTrace();
      assert (false);
    }
    return proc;
  }

  public static final String AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms";
  public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
  public static final String GROUP_ID = "group.id";
  public static final String ENABLE_AUTO_COMMIT = "enable.auto.commit";
  public static final String KEY_DESERIALIZER = "key.deserializer";
  public static final String VALUE_DESERIALIZER = "value.deserializer";
  public static final String SESSION_TIMEOUT_MS = "session.timeout.ms";

  public static Properties createKafkaConsumerConfig() {
    Properties props = new Properties();
    props.put(BOOTSTRAP_SERVERS, "testkafka040:9092");
    props.put(GROUP_ID, "singer_latency_test_" + SingerUtils.getHostname());
    props.put(ENABLE_AUTO_COMMIT, "false");
    props.put(AUTO_COMMIT_INTERVAL_MS, 500);
    props.put(SESSION_TIMEOUT_MS, 12000);
    props.put(KEY_DESERIALIZER, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put(VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    return props;
  }


  /**
   * Remove the test directories
   *
   * @param failOnException whether to fail the test in exception
   */
  public static void e2eTestCleanup(String testDir, boolean failOnException) {
    try {
      Process proc = Runtime.getRuntime().exec("rm -rf " + testDir);
      proc.waitFor();
    } catch (Exception e) {
      e.printStackTrace();
      assert (failOnException);
    }
  }
  
  public static void createLogStream(String logDir, String logName, int rotateThresholdKilobytes, int numMessages) {
    com.pinterest.singer.client.ThriftLogger logger = ThriftLoggerFactory.getLogger(logName, 24);
    try {
      for (int i = 0; i < numMessages; i++) {
        Event event = new Event();
        event.setTime(System.nanoTime());
        logger.append(event);
      }
      logger.close();
    } catch (Exception e) {
      e.printStackTrace();
      assert (false);
    }
  }
}
