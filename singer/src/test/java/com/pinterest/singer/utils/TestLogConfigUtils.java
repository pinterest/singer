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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.After;
import org.junit.Test;

import com.pinterest.singer.common.SingerConfigDef;
import com.pinterest.singer.config.ConfigFileWatcher;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.thrift.configuration.LogStreamProcessorConfig;
import com.pinterest.singer.thrift.configuration.MemqWriterConfig;
import com.pinterest.singer.thrift.configuration.RealpinWriterConfig;
import com.pinterest.singer.thrift.configuration.RegexBasedModifierConfig;
import com.pinterest.singer.thrift.configuration.S3WriterConfig;
import com.pinterest.singer.thrift.configuration.SamplingType;
import com.pinterest.singer.thrift.configuration.TextReaderConfig;

public class TestLogConfigUtils {

  @After
  public void after() {
    LogConfigUtils.SHADOW_MODE_ENABLED = false;
    LogConfigUtils.DEFAULT_SERVERSET_DIR = "/var/serverset";
  }

  @Test
  public void testRealPinTTLParsing() throws ConfigurationException {
    String CONFIG = "" + "topic=test\n" + "objectType=mobile_perf_log\n"
        + "serverSetPath=/xyz/realpin/prod";
    PropertiesConfiguration config = new PropertiesConfiguration();
    config.load(new ByteArrayInputStream(CONFIG.getBytes()));
    RealpinWriterConfig rpConf = LogConfigUtils.parseRealpinWriterConfig(config);
    // default TTL
    assertEquals(-1, rpConf.getTtl());

    CONFIG = "" + "topic=test\n" + "objectType=mobile_perf_log\n"
        + "serverSetPath=/xyz/realpin/prod\n" + "ttl=10000";
    config = new PropertiesConfiguration();
    config.load(new ByteArrayInputStream(CONFIG.getBytes()));
    rpConf = LogConfigUtils.parseRealpinWriterConfig(config);
    assertEquals(10000, rpConf.getTtl());
  }

  @Test
  public void testKafkaProducerConfigCatchBadCompression() throws ConfigurationException {
    Map<String, Object> map = new HashMap<>();
    map.put("bootstrap.servers", "test123");
    AbstractConfiguration config = new MapConfiguration(map);
    try {
      LogConfigUtils.parseProducerConfig(config);
    } catch (ConfigurationException e) {
      // fail since no exception should be thrown
      throw e;
    }

    map.put("compression.type", "abcd");
    try {
      LogConfigUtils.parseProducerConfig(config);
      fail("Must have thrown an exception since invalid compression type was passed");
    } catch (ConfigurationException e) {
    }

    for (String type : Arrays.asList("gzip", "snappy", "zstd", "lz4")) {
      map.put("compression.type", type);
      try {
        LogConfigUtils.parseProducerConfig(config);
      } catch (ConfigurationException e) {
        throw e;
      }
    }
  }

  @Test
  public void testKafkaProducerConfigCatchEmptyBrokerset() throws Exception {
    // set up empty serverset
    LogConfigUtils.DEFAULT_SERVERSET_DIR = "target/serversets";
    new File(LogConfigUtils.DEFAULT_SERVERSET_DIR).mkdirs();
    File emptyServerset = new File(LogConfigUtils.DEFAULT_SERVERSET_DIR + "/discovery.m10nkafka.prod");
    emptyServerset.createNewFile();
    //set up bad producer config
    Map<String, Object> map = new HashMap<>();
    map.put("metadata.broker.serverset", "/discovery/m10nkafka/prod");
    AbstractConfiguration config = new MapConfiguration(map);
    try {
      LogConfigUtils.parseProducerConfig(config);
      fail("Should throw exception");
    } catch (ConfigurationException ex) {
      assertEquals("serverset file is empty", ex.getMessage());
    }
  }

  @Test
  public void testKafkaProducerConfigCatchBadPartitioner() throws ConfigurationException {
    Map<String, Object> map = new HashMap<>();
    map.put("bootstrap.servers", "test123");
    AbstractConfiguration config = new MapConfiguration(map);
    try {
      LogConfigUtils.parseProducerConfig(config);
    } catch (ConfigurationException e) {
      // fail since no exception should be thrown
      throw e;
    }

    map.put("partitioner.class",
        "com.pinterest.singer.writer.partitioners.rc32ByteArrayPartitioner");
    try {
      LogConfigUtils.parseProducerConfig(config);
      fail("Must have thrown an exception since invalid partitioner class");
    } catch (ConfigurationException e) {
    }

    for (String type : Arrays.asList(
        "com.pinterest.singer.writer.partitioners.Crc32ByteArrayPartitioner",
        "com.pinterest.singer.writer.partitioners.DefaultPartitioner",
        "com.pinterest.singer.writer.partitioners.SimpleRoundRobinPartitioner",
        "com.pinterest.singer.writer.partitioners.SinglePartitionPartitioner",
        "com.pinterest.singer.writer.partitioners.LocalityAwareRandomPartitioner")) {
      map.put("partitioner.class", type);
      try {
        LogConfigUtils.parseProducerConfig(config);
      } catch (ConfigurationException e) {
        throw e;
      }
    }
  }

  @Test
  public void testBaseSingerConfig() throws ConfigurationException {
    Map<String, Object> map = new HashMap<>();
    map.put("monitor.monitorIntervalInSecs", "10");
    map.put("statsPusherHostPort", "localhost:1900");
    AbstractConfiguration config = new MapConfiguration(map);
    // defaults should be correct
    LogConfigUtils.parseCommonSingerConfigHeader(config);

    // must throw configuration exception
    map.put("statsPusherClass", "com.pinterest.singer.monitor.DefaultLogMonitor");
    try {
      LogConfigUtils.parseCommonSingerConfigHeader(config);
      fail(
          "Must fail since the supplied class is not a valid StatsPusher class but it is a valid class");
    } catch (Exception e) {
    }

    map.put("statsPusherClass", "com.pinterest.singer.monitor.Xyz");
    try {
      LogConfigUtils.parseCommonSingerConfigHeader(config);
      fail("Must fail since the supplied class is not a valid class");
    } catch (Exception e) {
    }
    map.remove("statsPusherClass");
    // cleanup after stats test

    map.put("environmentProviderClass", "com.pinterest.singer.monitor.Xyz");
    try {
      LogConfigUtils.parseCommonSingerConfigHeader(config);
      fail("Must fail since the supplied class is not a valid class");
    } catch (Exception e) {
    }
    map.put("environmentProviderClass", "com.pinterest.singer.monitor.DefaultLogMonitor");
    try {
      LogConfigUtils.parseCommonSingerConfigHeader(config);
      fail("Must fail since the supplied class is not a valid class");
    } catch (Exception e) {
    }
  }

  @Test
  public void testKafkaProducerConfigAck() throws ConfigurationException {
    Map<String, Object> map = new HashMap<>();
    map.put("bootstrap.servers", "localhost:9092");
    AbstractConfiguration config = new MapConfiguration(map);
    try {
      KafkaProducerConfig producerConfig = LogConfigUtils.parseProducerConfig(config);
      assertEquals(producerConfig.getAcks(), "1");
    } catch (ConfigurationException e) {
      // fail since no exception should be thrown
      throw e;
    }

    map.put(SingerConfigDef.REQUEST_REQUIRED_ACKS, "1");
    try {
      KafkaProducerConfig producerConfig = LogConfigUtils.parseProducerConfig(config);
      assertEquals(producerConfig.getAcks(), "1");
    } catch (ConfigurationException e) {
      // fail since no exception should be thrown
      throw e;
    }

    map.put(SingerConfigDef.REQUEST_REQUIRED_ACKS, "-1");
    config = new MapConfiguration(map);
    try {
      KafkaProducerConfig producerConfig = LogConfigUtils.parseProducerConfig(config);
      assertEquals(producerConfig.getAcks(), "-1");
      KafkaUtils.createKafkaProducer(producerConfig);
    } catch (ConfigurationException e) {
      // fail since no exception should be thrown
      throw e;
    }

    map.put(SingerConfigDef.REQUEST_REQUIRED_ACKS, "2");
    try {
      LogConfigUtils.parseProducerConfig(config);
      fail("Must have thrown an exception since invalid ack config was passed");
    } catch (ConfigurationException e) {
    }

    // test that "producer.acks" override "producer.request.required.acks" setting
    map.put(SingerConfigDef.ACKS, "all");
    config = new MapConfiguration(map);
    try {
      KafkaProducerConfig producerConfig = LogConfigUtils.parseProducerConfig(config);
      assertEquals(producerConfig.getAcks(), "all");
      KafkaUtils.createKafkaProducer(producerConfig);
    } catch (ConfigurationException e) {
      // fail since no exception should be thrown
      throw e;
    }
  }

  @Test
  public void testTextLogEnvInjection() throws ConfigurationException {
    // NOTE: this test requires at least 2 environment variables to be present
    Map<String, String> envs = System.getenv();
    List<Entry<String, String>> envList = new ArrayList<>();
    for (Entry<String, String> entry : envs.entrySet()) {
      envList.add(entry);
    }
    String config = "reader.type=text\n" + "reader.text.readerBufferSize=131072\n"
        + "reader.text.maxMessageSize=131072\n" + "reader.text.messageStartRegex=^.*$\n"
        + "reader.text.numMessagesPerLogMessage=1\n" + "reader.text.logMessageType=plain_text\n"
        + "reader.text.prependEnvironmentVariables=" + envList.get(0).getKey() + "|"
        + envList.get(1).getKey() + "\n" + "reader.text.prependHostname=true\n"
        + "reader.text.prependTimestamp=true";
    PropertiesConfiguration configObj = new PropertiesConfiguration();
    configObj.load(new StringReader(config));
    String val = configObj.getString("reader.text.prependEnvironmentVariables");
    assertEquals(envList.get(0).getKey() + "|" + envList.get(1).getKey(), val);
    TextReaderConfig textConfig = LogConfigUtils
        .parseTextReaderConfig((AbstractConfiguration) configObj.subset("reader.text"));
    Map<String, ByteBuffer> env = textConfig.getEnvironmentVariables();
    assertEquals(2, env.size());
  }

  @Test
  public void testShadowKafkaProducerConfigs() throws ConfigurationException, IOException {
    LogConfigUtils.DEFAULT_SERVERSET_DIR = "target/serversets";
    new File(LogConfigUtils.DEFAULT_SERVERSET_DIR).mkdirs();
    Files.write(new File(LogConfigUtils.DEFAULT_SERVERSET_DIR + "/discovery.xyz.prod").toPath(),
        "xyz:9092".getBytes());
    Files.write(new File(LogConfigUtils.DEFAULT_SERVERSET_DIR + "/discovery.test.prod").toPath(),
        "test:9092".getBytes());
    Files.write(new File(LogConfigUtils.DEFAULT_SERVERSET_DIR + "/discovery.test2.prod").toPath(),
        "test2:9092".getBytes());
    Map<String, Object> map = new HashMap<>();
    map.put(SingerConfigDef.BROKER_SERVERSET_DEPRECATED, "/discovery/xyz/prod");
    AbstractConfiguration config = new MapConfiguration(map);

    KafkaProducerConfig producerConfig = null;
    // without shadow mode activation regular serverset file should be sourced
    producerConfig = LogConfigUtils.parseProducerConfig(config);
    producerConfig = LogConfigUtils.parseProducerConfig(config);
    assertEquals(new HashSet<>(Arrays.asList("xyz:9092")), new HashSet<>(producerConfig.getBrokerLists()));

    // with shadow mode override should be sourced
    LogConfigUtils.SHADOW_MODE_ENABLED = true;
    LogConfigUtils.SHADOW_SERVERSET_MAPPING.put(LogConfigUtils.DEFAULT_SHADOW_SERVERSET,
        "discovery.test.prod");

    producerConfig = LogConfigUtils.parseProducerConfig(config);
    assertEquals(new HashSet<>(Arrays.asList("test:9092")), new HashSet<>(producerConfig.getBrokerLists()));

    LogConfigUtils.SHADOW_SERVERSET_MAPPING.put("/discovery/xyz/prod", "discovery.test2.prod");
    producerConfig = LogConfigUtils.parseProducerConfig(config);
    assertEquals(new HashSet<>(Arrays.asList("test2:9092")), new HashSet<>(producerConfig.getBrokerLists()));
  }

  @Test
  public void testProducerBufferMemory() throws ConfigurationException {
    Map<String, Object> map = new HashMap<>();
    map.put("bootstrap.servers", "localhost:9092");
    map.put("buffer.memory", "2048");
    AbstractConfiguration config = new MapConfiguration(map);
    try {
      KafkaProducerConfig producerConfig = LogConfigUtils.parseProducerConfig(config);
      assertEquals(2048, producerConfig.getBufferMemory());
    } catch (ConfigurationException e) {
      // fail since no exception should be thrown
      throw e;
    }
  }

  @Test
  public void testProducerLingerMs() throws ConfigurationException {
    Map<String, Object> map = new HashMap<>();
    map.put("bootstrap.servers", "localhost:9092");
    map.put("linger.ms", "300");
    AbstractConfiguration config = new MapConfiguration(map);
    try {
      KafkaProducerConfig producerConfig = LogConfigUtils.parseProducerConfig(config);
      assertTrue(producerConfig.isSetLingerMs());
      assertEquals(300, producerConfig.getLingerMs());
    } catch (ConfigurationException e) {
      // fail since no exception should be thrown
      throw e;
    }

    map.remove("linger.ms");
    config = new MapConfiguration(map);
    try {
      KafkaProducerConfig producerConfig = LogConfigUtils.parseProducerConfig(config);
      assertFalse(producerConfig.isSetLingerMs());
      assertEquals(10, producerConfig.getLingerMs());
    } catch (ConfigurationException e) {
      // fail since no exception should be thrown
      throw e;
    }
  }

  @Test
  public void testgetRandomizedStartOffsetBrokers() throws ConfigurationException{
    List<String> one = LogConfigUtils.getRandomizedStartOffsetBrokers(101,
        new LinkedHashSet<>(Arrays.asList("1", "2", "3", "4")));
    List<String> two = LogConfigUtils.getRandomizedStartOffsetBrokers(101,
        new LinkedHashSet<>(Arrays.asList("1", "2", "3", "4")));
    assertEquals(one, two);
  }

  @Test
  public void testMemqConfigurations() throws Exception {
    LogConfigUtils.DEFAULT_SERVERSET_DIR = "target";
    String config = "type=memq\n" + "memq.topic=test2\n" + "memq.cluster=prototype\n"
        + "memq.environment=dev\n" + "memq.compression=zstd\n" + "memq.maxInFlightRequests=60\n"
        + "memq.disableAcks=false\n" + "memq.maxPayLoadBytes=2010000\n" + "memq.clientType=tcp\n"
        + "memq.auditor.enabled=true\n" + "memq.auditor.topic=auditTopic\n"
        + "memq.auditor.class=com.pinterest.memq.client.commons.audit.KafkaBackedAuditor\n"
        + "memq.auditor.serverset=/var/serverset/discovery.testkafka.prod";
    PropertiesConfiguration conf = new PropertiesConfiguration();
    conf.load(new ByteArrayInputStream(config.getBytes()));

    String pathname = "target/discovery.memq.dev.prototype.prod_rich_data";
    File file = new File(pathname);
    Path path = file.toPath();
    if (file.exists()) {
      Files.delete(path);
    }
    Files.write(path, "test".getBytes());

    MemqWriterConfig writerConfig = LogConfigUtils.parseLogStreamWriterConfig(conf)
        .getMemqWriterConfig();
    assertNotNull(writerConfig);
    assertEquals("prototype", writerConfig.getCluster());
    assertEquals("test2", writerConfig.getTopic());
    assertNotNull(writerConfig.getAuditorConfig());
    assertEquals("target/discovery.memq.dev.prototype.prod_rich_data", writerConfig.getServerset());
  }

  @Test
  public void testMemqServersetParsing() throws Exception {
    LogConfigUtils.DEFAULT_SERVERSET_DIR = "target";
    String config = "type=memq\n" + "memq.topic=test2\n" + "memq.cluster=prototype\n"
        + "memq.environment=dev\n" + "memq.compression=zstd\n" + "memq.maxInFlightRequests=60\n"
        + "memq.disableAcks=false\n" + "memq.maxPayLoadBytes=2010000\n" + "memq.clientType=tcp\n"
        + "memq.auditor.enabled=true\n" + "memq.auditor.topic=auditTopic\n"
        + "memq.auditor.class=com.pinterest.memq.client.commons.audit.KafkaBackedAuditor\n"
        + "memq.auditor.serverset=/var/serverset/discovery.testkafka.prod";
    PropertiesConfiguration conf = new PropertiesConfiguration();
    conf.load(new ByteArrayInputStream(config.getBytes()));

    String pathname = "target/discovery.memq.dev.prototype.prod_rich_data";
    File file = new File(pathname);
    Path path = file.toPath();
    if (file.exists()) {
      Files.delete(path);
    }
    Files.write(path, "test".getBytes());

    MemqWriterConfig writerConfig = LogConfigUtils.parseLogStreamWriterConfig(conf)
        .getMemqWriterConfig();
    assertNotNull(writerConfig);
    assertEquals("prototype", writerConfig.getCluster());
    assertEquals("test2", writerConfig.getTopic());
    assertNotNull(writerConfig.getAuditorConfig());
    assertEquals("target/discovery.memq.dev.prototype.prod_rich_data", writerConfig.getServerset());

    List<?> l = ConfigFileWatcher.defaultInstance().getWatchers(pathname);
    assertEquals(1, l.size());

    for (int i = 0; i < 10; i++) {
      LogConfigUtils.parseLogStreamWriterConfig(conf).getMemqWriterConfig();
    }

    assertEquals(1, l.size());
  }

  @Test
  public void testParseLogStreamProcessorConfig() {
    PropertiesConfiguration conf = new PropertiesConfiguration();
    conf.setProperty(SingerConfigDef.PROCESS_BATCH_SIZE, "500");
    conf.setProperty(SingerConfigDef.PROCESS_INTERVAL_MILLIS, "1000");
    conf.setProperty(SingerConfigDef.PROCESS_INTERVAL_MILLIS_MAX, "5000");
    conf.setProperty(SingerConfigDef.PROCESS_TIME_SLICE_SECS, "15");
    conf.setProperty(SingerConfigDef.PROCESS_ENABLE_MEMORY_EFFICIENCY, "true");
    LogStreamProcessorConfig lspc = LogConfigUtils.parseLogStreamProcessorConfig(conf);
    assertEquals(500, lspc.getBatchSize());
    assertEquals(1000, lspc.getProcessingIntervalInMillisecondsMin());
    assertEquals(5000, lspc.getProcessingIntervalInMillisecondsMax());
    assertEquals(15000, lspc.getProcessingTimeSliceInMilliseconds());
    assertEquals(SamplingType.NONE, lspc.getDeciderBasedSampling());
    assertTrue(lspc.isEnableMemoryEfficientProcessor());

    conf.setProperty(SingerConfigDef.PROCESS_DECIDER_BASED_SAMPLING, "instance");
    lspc = LogConfigUtils.parseLogStreamProcessorConfig(conf);
    assertEquals(SamplingType.INSTANCE, lspc.getDeciderBasedSampling());

    // deciderBasedSampling has priority over enableDeciderBasedSampling
    conf.setProperty(SingerConfigDef.PROCESS_ENABLE_DECIDER_BASED_SAMPLING_SAMPLING, "true");
    lspc = LogConfigUtils.parseLogStreamProcessorConfig(conf);
    assertEquals(SamplingType.INSTANCE, lspc.getDeciderBasedSampling());

    // deciderBasedSampling not set, enableDeciderBasedSampling will take effect
    conf.clearProperty(SingerConfigDef.PROCESS_DECIDER_BASED_SAMPLING);
    lspc = LogConfigUtils.parseLogStreamProcessorConfig(conf);
    assertEquals(SamplingType.MESSAGE, lspc.getDeciderBasedSampling());
  }

  @Test
  public void testS3WriterConfigurations() throws Exception {
    String
        config =
        "type=s3\n" + "s3.bucket=my-fav-bucket\n" + "s3.keyFormat=%{service}/%{index}/my_log\n"
            + "s3.maxFileSizeMB=100\n" + "s3.minUploadTimeInSeconds=1\n" + "s3.maxRetries=10\n"
            + "s3.filenamePattern=^(?<service>[a-zA-Z0-9]+)_.*_(?<index>\\\\d+)\\\\.log$\n"
            + "s3.cannedAcl=bucket-owner-full-control";
    PropertiesConfiguration conf = new PropertiesConfiguration();
    conf.load(new ByteArrayInputStream(config.getBytes()));
    List<String> tokens = new ArrayList<>();
    tokens.add("service");
    tokens.add("index");
    S3WriterConfig
        s3WriterConfig =
        LogConfigUtils.parseLogStreamWriterConfig(conf).getS3WriterConfig();
    assertNotNull(s3WriterConfig);
    assertEquals(tokens.toString(), s3WriterConfig.getFilenameTokens().toString());
    assertEquals(100, s3WriterConfig.getMaxFileSizeMB());
    assertEquals(30, s3WriterConfig.getMinUploadTimeInSeconds());
    assertEquals(10, s3WriterConfig.getMaxRetries());
  }

  @Test
  public void testRegexTransformerConfigurations() throws Exception {
    String
        config =
        "type=regex_based_modifier\n"
            + "modifiedMessageFormat={log_level: $2\\, message: $4\\, timestamp: $1}\n"
            + "regex_based_modifier.encoding=UTF-8\n"
            + "regex=some_regex\n";
    PropertiesConfiguration conf = new PropertiesConfiguration();
    conf.load(new ByteArrayInputStream(config.getBytes()));

    RegexBasedModifierConfig
        regexBasedModifierConfig =
        LogConfigUtils.parseRegexBasedModifierConfig(conf);

    assertNotNull(regexBasedModifierConfig);
  }
}
