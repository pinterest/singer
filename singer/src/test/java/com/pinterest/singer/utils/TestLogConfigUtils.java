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
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

import com.pinterest.singer.common.SingerConfigDef;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.thrift.configuration.RealpinWriterConfig;

public class TestLogConfigUtils {

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
      fail(
          "Must fail since the supplied class is not a valid class");
    } catch (Exception e) {
    }
    map.remove("statsPusherClass");
    // cleanup after stats test
    
    map.put("environmentProviderClass", "com.pinterest.singer.monitor.Xyz");
    try {
      LogConfigUtils.parseCommonSingerConfigHeader(config);
      fail(
          "Must fail since the supplied class is not a valid class");
    } catch (Exception e) {
    }
    map.put("environmentProviderClass", "com.pinterest.singer.monitor.DefaultLogMonitor");
    try {
      LogConfigUtils.parseCommonSingerConfigHeader(config);
      fail(
          "Must fail since the supplied class is not a valid class");
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
}
