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
package com.pinterest.singer.config;

import com.pinterest.singer.SingerTestBase;
import com.pinterest.singer.common.SingerConfigDef;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.utils.LogConfigUtils;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.ConfigurationException;

/**
 * Test case for DirectorySingerConfigurator.
 */
@RunWith(JUnit4.class)
public class DirectorySingerConfiguratorTest extends SingerTestBase {

  private static final String IS_MISSING_ELEMENT = " is required for Singer Configuration";

  @Test
  public void testConfiguratorLoadConfigsAndReceiveLiveChanges() throws Exception {
    dumpServerSetFiles();
    // Make the singer config property file
    File singerConfigFile = createSingerConfigFile(makeDirectorySingerConfigProperties());
    // Make two log config properties files in the logConfigDir.
    createLogConfigPropertiesFile("project.logstream1.properties", ImmutableMap.of(
        "writer.kafka.producerConfig.bootstrap.servers", "127.0.0.1:9092"));
    createLogConfigPropertiesFile("project.logstream2.properties",
        ImmutableMap.of("writer.kafka.producerConfig.bootstrap.servers", "127.0.0.1:9092"));

    // Check the configurator can load two log configs.
    DirectorySingerConfigurator configurator = new DirectorySingerConfigurator(singerConfigFile
        .getParent());
    SingerConfig singerConfig = configurator.parseSingerConfig();
    final AtomicInteger exitCode = new AtomicInteger(-1);
    new SingerDirectoryWatcher(configurator.parseSingerConfig()
        , configurator, new SingerDirectoryWatcher.ExitManager() {
      @Override
      public void exit(int status) {
        exitCode.set(status);
      }
    });
    assertEquals(2, singerConfig.getLogConfigsSize());
    SingerLogConfig logConfig = singerConfig.logConfigs.get(0);
    assertEquals("project.logstream1", logConfig.getName());
    assertEquals("/mnt/log/singer", logConfig.getLogDir());
    SingerLogConfig logConfig1 = singerConfig.logConfigs.get(1);
    assertEquals("project.logstream2", logConfig1.getName());
    assertEquals("/mnt/log/singer", logConfig1.getLogDir());

    {
      // Test live changes : adding a new file.
      createLogConfigPropertiesFile("project.logstream3.properties", ImmutableMap.of(
          "writer.kafka.producerConfig.bootstrap.servers", "127.0.0.1:9092"));
      Thread.sleep(2000);
      // check exit() called with code 0.
      assertEquals(0, exitCode.get());
      // simulate restart: recreate a configurator and check now it has three configs.
      DirectorySingerConfigurator newConfigurator = new DirectorySingerConfigurator
          (singerConfigFile.getParent());
      assertEquals(3, newConfigurator.parseSingerConfig().getLogConfigsSize());
    }
    exitCode.set(-1);
    {
      // Test live changes : modifying a config to increase batch size from 200 to 300.
      createLogConfigPropertiesFile("project.logstream1.properties", ImmutableMap.of("processor.batchSize",
          "300", "writer.kafka.producerConfig.bootstrap.servers", "127.0.0.1:9092"));
      Thread.sleep(2000);
      assertEquals(0, exitCode.get());
    }
  }

  @Test
  public void testValidateConfigChanges() throws Exception {
    // Make the singer config property file
    File singerConfigFile = createSingerConfigFile(makeWrongSingerConfigProperties(4));

    // Check the configurator can load two log configs.
    DirectorySingerConfigurator configurator = new DirectorySingerConfigurator(singerConfigFile.getParent());
    try {
      configurator.parseSingerConfig();
      assertEquals(true,false);
    } catch (ConfigurationException x) {
      assertEquals(SingerConfigDef.MONITOR_INTERVAL_IN_SECS + IS_MISSING_ELEMENT, x.getMessage());
    }

    File singerConfigFileTwo = createSingerConfigFile(makeWrongSingerConfigProperties(5));
    DirectorySingerConfigurator configuratorTwo = new DirectorySingerConfigurator(singerConfigFileTwo
            .getParent());
    try {
      configuratorTwo.parseSingerConfig();
      assertEquals(true,false);
    } catch (ConfigurationException x) {
      assertEquals("singer.setLogConfigPollIntervalSecs" + IS_MISSING_ELEMENT, x.getMessage());
    }

    File singerConfigFileThree = createSingerConfigFile(makeWrongSingerConfigProperties(3));
    DirectorySingerConfigurator configuratorThree = new DirectorySingerConfigurator(singerConfigFileThree
            .getParent());
    try {
      configuratorThree.parseSingerConfig();
      assertEquals(true,false);
    } catch (ConfigurationException x) {
      assertEquals("monitorIntervalInSecs" + IS_MISSING_ELEMENT + "\n" +
              "singer.setLogConfigPollIntervalSecs" + IS_MISSING_ELEMENT, x.getMessage());
    }
  }

  @Test
  public void testLoadNewConfig() throws Exception {
    String newConfig = createNewLogConfigString();
    SingerLogConfig[] logConfigs = LogConfigUtils.parseLogStreamConfigFromFile(newConfig);

    assertEquals(2, logConfigs.length);
    SingerLogConfig topic1 = logConfigs[0];
    assertEquals("topic1", topic1.getName());
    assertEquals("/mnt/thrift_logger", topic1.getLogDir());
    SingerLogConfig topic2 = logConfigs[1];
    assertEquals("topic2", topic2.getName());
    assertEquals("/mnt/thrift_logger", topic2.getLogDir());
  }

  @Test
  public void testConfiguratorOverride() throws Exception {
    dumpServerSetFiles();
    // Make the singer config property file
    File dir = Files.createTempDirectory("overrideconfigdir").toFile();
    dir.deleteOnExit();
    File test1Override = Files.createTempFile(dir.toPath(), "test1", ".properties").toFile();
    Map<String, String> override1 = new HashMap<>();
    override1.put("match.config.name", "writer.type");
    override1.put("match.config.value", "kafka");
    override1.put("override.writer.kafka.producerConfig.buffer.memory", "1");
    createPropertyFile(test1Override.getAbsolutePath(), override1);
    test1Override.deleteOnExit();

    File test2Override = Files.createTempFile(dir.toPath(), "test2", ".properties").toFile();
    Map<String, String> override2 = new HashMap<>();
    override2.put("match.config.name", "writer.kafka.producerConfig.bootstrap.servers");
    override2.put("match.config.value", "127.0.0.2:9092");
    override2.put("override.writer.kafka.producerConfig.buffer.memory", "2");
    override2.put("override.writer.kafka.producerConfig.max.request.size", "3");
    createPropertyFile(test2Override.getAbsolutePath(), override2);
    test2Override.deleteOnExit();

    Map<String, String> props = makeDirectorySingerConfigProperties();
    props.put("singer.configOverrideDir", dir.getAbsolutePath());

    File singerConfigFile = createSingerConfigFile(props);
    // Make two log config properties files in the logConfigDir.
    createLogConfigPropertiesFile("project.logstream1.properties", ImmutableMap.of(
        "writer.kafka.producerConfig.bootstrap.servers", "127.0.0.2:9092"));
    createLogConfigPropertiesFile("project.logstream2.properties",
        ImmutableMap.of("writer.kafka.producerConfig.bootstrap.servers", "127.0.0.1:9092"));


    // Check the configurator can load two log configs.
    DirectorySingerConfigurator configurator = new DirectorySingerConfigurator(singerConfigFile
        .getParent());
    SingerConfig singerConfig = configurator.parseSingerConfig();
    assertTrue(singerConfig.isSetConfigOverrideDir());

    assertEquals(2, singerConfig.getLogConfigsSize());
    SingerLogConfig logConfig = singerConfig.logConfigs.get(0);
    assertEquals("project.logstream1", logConfig.getName());
    assertEquals(2, logConfig.getLogStreamWriterConfig().getKafkaWriterConfig().getProducerConfig().getBufferMemory());
    assertEquals(3, logConfig.getLogStreamWriterConfig().getKafkaWriterConfig().getProducerConfig().getMaxRequestSize());
    SingerLogConfig logConfig1 = singerConfig.logConfigs.get(1);
    assertEquals("project.logstream2", logConfig1.getName());
    assertEquals(1, logConfig1.getLogStreamWriterConfig().getKafkaWriterConfig().getProducerConfig().getBufferMemory());
  }

  @Test
  public void testSkipBadConfigEmptyServerset() throws Exception {
    dumpServerSetFiles();
    // create serverset dir + file
    String path = "discovery.m10nkafka.prod";
    LogConfigUtils.DEFAULT_SERVERSET_DIR = "target/serversets";
    new File(LogConfigUtils.DEFAULT_SERVERSET_DIR).mkdirs();
    File emptyServerset = new File(LogConfigUtils.DEFAULT_SERVERSET_DIR + path);
    if (!emptyServerset.createNewFile()) { System.out.println("File already exists"); }

    File singerConfigFile = createSingerConfigFile(makeDirectorySingerConfigProperties());
    // Bad log config (raises /0 exception)
    createLogConfigPropertiesFile("project.logstream1.properties");
    // Good log config
    createLogConfigPropertiesFile("project.logstream2.properties",
            ImmutableMap.of("writer.kafka.producerConfig.bootstrap.servers", "127.0.0.1:9092"));

    // Check the configurator can load two log configs.
    DirectorySingerConfigurator configurator = new DirectorySingerConfigurator(singerConfigFile
            .getParent());
    SingerConfig singerConfig = configurator.parseSingerConfig();
    // There should only be one config
    assertEquals(1, singerConfig.getLogConfigsSize());
    LogConfigUtils.DEFAULT_SERVERSET_DIR = "/var/serverset";
  }
  @Test
  public void testSkipBadConfigConversionError() throws Exception {
    dumpServerSetFiles();
    // create serverset dir + file
    File singerConfigFile = createSingerConfigFile(makeDirectorySingerConfigProperties());
    // Bad log config (conversion error in max.request.size [non-int])
    createLogConfigPropertiesFile("project.logstream1.properties",
            ImmutableMap.of("writer.kafka.producerConfig.bootstrap.servers", "127.0.0.1:9092", "writer.kafka.producerConfig.max.request.size", "1111E2"));
    // Good log config
    createLogConfigPropertiesFile("project.logstream2.properties",
            ImmutableMap.of("writer.kafka.producerConfig.bootstrap.servers", "127.0.0.1:9092"));

    // Check the configurator can load two log configs.
    DirectorySingerConfigurator configurator = new DirectorySingerConfigurator(singerConfigFile
            .getParent());
    SingerConfig singerConfig = configurator.parseSingerConfig();
    // There should only be one config
    assertEquals(1, singerConfig.getLogConfigsSize());
  }

  Map<String, String> makeWrongSingerConfigProperties(int propertyNum) {

    if (propertyNum == 4) {
      return new TreeMap<String, String>() {
        private static final long serialVersionUID = 1L;

        {
          put("singer.threadPoolSize", "8");
          put("singer.ostrichPort", "9896");
          put("singer.logConfigDir", logConfigDir.getPath());
          put("singer.logConfigPollIntervalSecs", "1");
        }
      };
    }
    else if (propertyNum == 5) {
      return new TreeMap<String, String>() {
        private static final long serialVersionUID = 1L;
        {
          put("singer.threadPoolSize", "8");
          put("singer.ostrichPort", "9896");
          put("singer.monitor.monitorIntervalInSecs", "10");
          put("singer.logConfigDir", logConfigDir.getPath());
        }
      };
    }
    else {
      return new TreeMap<String, String>() {
        private static final long serialVersionUID = 1L;
        {
          put("singer.threadPoolSize", "8");
          put("singer.ostrichPort", "9896");
          put("singer.logConfigDir", logConfigDir.getPath());
        }
      };
    }
  }

  Map<String, String> makeDirectorySingerConfigProperties() {
    return new TreeMap<String, String>() {
      private static final long serialVersionUID = 1L;
      {
        put("singer.threadPoolSize", "8");
        put("singer.ostrichPort", "9896");
        put("singer.monitor.monitorIntervalInSecs", "10");
        put("singer.logConfigDir", logConfigDir.getPath());
        put("singer.logConfigPollIntervalSecs", "1");
        put("singer.logRetentionInSecs", "172800");
        put("singer.heartbeatEnabled", "false");
      }
    };
  }
}
