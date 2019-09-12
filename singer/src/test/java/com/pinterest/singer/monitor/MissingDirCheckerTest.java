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
package com.pinterest.singer.monitor;

import com.pinterest.singer.SingerTestBase;
import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.config.DirectorySingerConfigurator;
import com.pinterest.singer.thrift.configuration.SingerConfig;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


public class MissingDirCheckerTest extends SingerTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(MissingDirCheckerTest.class);

  @Before
  public void setUp() throws SecurityException, NoSuchFieldException, IllegalArgumentException,
                             IllegalAccessException, IOException {
    tempDir.create();
    logConfigDir = tempDir.newFolder(DirectorySingerConfigurator.SINGER_LOG_CONFIG_DIR);

    LOG.info("Get and reset LogStreamManager instance (singleton) to make sure that instance used"
        + " in unit test is specifically set for this unit test");
    LogStreamManager.getInstance();
    LogStreamManager.reset();
  }

  @After
  public void tearDown() throws IOException {
    File testBaseDir= tempDir.getRoot();
    LOG.info("Clean up files under test base dir: " + testBaseDir.getAbsolutePath());
    if (testBaseDir.exists()) {
      FileUtils.deleteDirectory(testBaseDir);
    }
  }

  /**
   *  This test is to verify MissingDirChecker can track all SingerLog whose log dir was not
   *  created before Singer is started. After log dir for certain SingerLog was created,
   *  MissingDirChecker will call method to properly initialize log stream for this SingerLog and
   *  remove this SingerLog from the tracking hash map maintained by MissingDirChecker.
   *
   *  If this test fails, it might be the issue that after log dir was created for certain
   *  SingerLog, MissingDirChecker has not had chance to call method to initialize log stream and
   *  remove this SingerLog from the hash map. One fix is to let the Thread sleep longer, eg:
   *  Thread.sleep(instance.getMissingDirChecker().getSleepInMills() * 5);
   * @throws Exception
   */
  @Test
  public void testMissingDirChecker() throws Exception{
    String testBasePath = tempDir.getRoot().getAbsolutePath();

    LOG.info("Create test singer property file and singer log config property file.");
    Map<String, String> singerConfigProperties = makeDirectorySingerConfigProperties("");
    File singerConfigFile = createSingerConfigFile(singerConfigProperties);
    String[] propertyFileNames = {"test.app1.properties",  "test.app2.properties",
                                  "test.app3.properties", "test.app4.properties",};
    String[] logStreamRegexes = {"app1_(\\\\w+)", "app2_(\\\\w+)", "app3_(\\\\w+)", "app4_(\\\\w+)"};
    String[] logDirPaths ={"/mnt/log/singer/", "/mnt/thrift_logger/", "/x/y/z/", "/a/b/c"};
    String[] topicNames = {"topic1", "topic2", "topic3", "topic4"};
    File[] files = new File[logDirPaths.length];
    for(int i = 0; i < logDirPaths.length; i++) {
      logDirPaths[i] = new File(testBasePath, logDirPaths[i]).getAbsolutePath();
      files[i] = createSingerLogConfigPropertyFile(propertyFileNames[i], logStreamRegexes[i],
          logDirPaths[i], topicNames[i]);
    }

    LOG.info("Try to parse SingerConfig.");
    String parentDirPath = singerConfigFile.getParent();
    DirectorySingerConfigurator configurator = new DirectorySingerConfigurator(parentDirPath);
    SingerConfig singerConfig = configurator.parseSingerConfig();

    LOG.info("Create LogStreamManager instance (Singleton).");
    SingerSettings.setSingerConfig(singerConfig);
    LogStreamManager instance = LogStreamManager.getInstance();
    LogStreamManager.initializeLogStreams();

    // setting sleep time of MissingDirChecker to be 5 seconds in order to accelerate unit test.
    instance.getMissingDirChecker().setSleepInMills(5000);

    int numOfSingerLogsWithoutDir = logDirPaths.length;
    assertEquals(numOfSingerLogsWithoutDir, instance.getMissingDirChecker().getSingerLogsWithoutDir().size());

    LOG.info("Verify MissingDirChecker runs properly while logDir is created for each SingerLog");
    for(String path : logDirPaths) {
      File f1 = new File(path);
      f1.mkdirs();
      assertTrue(f1.exists() && f1.isDirectory());
      Thread.sleep(instance.getMissingDirChecker().getSleepInMills() * 2);
      numOfSingerLogsWithoutDir -= 1;
      LOG.info("verify the number match: {} {}", numOfSingerLogsWithoutDir,
          instance.getMissingDirChecker().getSingerLogsWithoutDir().size());
      assertEquals(numOfSingerLogsWithoutDir, instance.getMissingDirChecker().getSingerLogsWithoutDir().size());
    }
    LOG.info("Verify MissingDirChecker thread can be stopped properly.");
    assertFalse(instance.getMissingDirChecker().getCancelled().get());
    instance.stop();
    assertTrue(instance.getMissingDirChecker().getCancelled().get());

  }

  public Map<String, String> makeDirectorySingerConfigProperties(String logConfigDirPath) {
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

  public File createSingerLogConfigPropertyFile(String propertyFileName,
                                                String logStreamRegex,
                                                String logDirPath,
                                                String topicName) throws IOException{
    Map<String, String> override = new HashMap<String, String>();
    override.put("writer.kafka.producerConfig.bootstrap.servers", "127.0.0.1:9092");
    override.put("logDir", logDirPath);
    override.put("logStreamRegex", logStreamRegex);
    override.put("writer.kafka.topic", topicName);
    return  createLogConfigPropertiesFile(propertyFileName, override);
  }

}


