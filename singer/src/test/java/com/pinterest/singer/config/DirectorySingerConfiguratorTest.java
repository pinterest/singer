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
    createLogConfigPropertiesFile("ads.mohawk.properties", ImmutableMap.of(
        "writer.kafka.producerConfig.bootstrap.servers", "127.0.0.1:9092"));
    File searchLogConfig = createLogConfigPropertiesFile("search.discovery.properties",
        ImmutableMap.of("writer.kafka.producerConfig.bootstrap.servers", "127.0.0.1:9092"));

    // Check the configurator can load two log configs.
    DirectorySingerConfigurator configurator = new DirectorySingerConfigurator(singerConfigFile
        .getParent());
    SingerConfig singerConfig = configurator.parseSingerConfig();
    final AtomicInteger exitCode = new AtomicInteger(-1);
    SingerDirectoryWatcher watcher = new SingerDirectoryWatcher(configurator.parseSingerConfig()
        , configurator, new SingerDirectoryWatcher.ExitManager() {
      @Override
      public void exit(int status) {
        exitCode.set(status);
      }
    });
    assertEquals(2, singerConfig.getLogConfigsSize());
    SingerLogConfig adsMohawk = singerConfig.logConfigs.get(0);
    assertEquals("ads.mohawk", adsMohawk.getName());
    assertEquals("/mnt/log/singer", adsMohawk.getLogDir());
    SingerLogConfig search = singerConfig.logConfigs.get(1);
    assertEquals("search.discovery", search.getName());
    assertEquals("/mnt/log/singer", search.getLogDir());

    {
      // Test live changes : adding a new file.
      createLogConfigPropertiesFile("ads.ads.properties", ImmutableMap.of(
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
      createLogConfigPropertiesFile("ads.mohawk.properties", ImmutableMap.of("processor.batchSize",
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
      SingerConfig singerConfig = configurator.parseSingerConfig();
      assertEquals(true,false);
    } catch (ConfigurationException x) {
      assertEquals(SingerConfigDef.MONITOR_INTERVAL_IN_SECS + IS_MISSING_ELEMENT, x.getMessage());
    }

    File singerConfigFileTwo = createSingerConfigFile(makeWrongSingerConfigProperties(5));
    DirectorySingerConfigurator configuratorTwo = new DirectorySingerConfigurator(singerConfigFileTwo
            .getParent());
    try {
      SingerConfig singerConfigTwo = configuratorTwo.parseSingerConfig();
      assertEquals(true,false);
    } catch (ConfigurationException x) {
      assertEquals("singer.setLogConfigPollIntervalSecs" + IS_MISSING_ELEMENT, x.getMessage());
    }

    File singerConfigFileThree = createSingerConfigFile(makeWrongSingerConfigProperties(3));
    DirectorySingerConfigurator configuratorThree = new DirectorySingerConfigurator(singerConfigFileThree
            .getParent());
    try {
      SingerConfig singerConfigThree = configuratorThree.parseSingerConfig();
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

  Map<String, String> makeWrongSingerConfigProperties(int propertyNum) {

    if (propertyNum == 4) {
      return new TreeMap<String, String>() {
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
