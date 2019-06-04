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

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerLog;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.thrift.LogFile;
import com.pinterest.singer.thrift.LogFileAndPath;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.utils.SingerUtils;

import java.util.ArrayList;
import java.util.stream.Collectors;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FileSystemMonitorTest extends com.pinterest.singer.SingerTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemMonitorTest.class);

  private void verifyFiles(String[] subFiles, LogStream stream) {
    List<LogFileAndPath> logFileAndPaths = stream.getLogFileAndPaths();

    LOG.info("subFiles.length = {}, logFilesAndPaths.size() = {}",
        subFiles.length, logFileAndPaths.size());
    assertEquals("There should be the same number of files in the directory and in the inodes list",
        subFiles.length, logFileAndPaths.size());

    for (String s : subFiles) {
      assertTrue(s + " should be in inodes found after creation", stream.containsFile(s));
    }

    Set<Long> inodeVals = new HashSet<>();
    for (int i = 0; i < logFileAndPaths.size(); i++) {
      inodeVals.add(logFileAndPaths.get(i).logFile.getInode());
    }
    assertEquals("The inodes should be unique", inodeVals.size(), logFileAndPaths.size());
  }

  @Test
  public void testTwoLogStreamsInSameDir() throws Exception {
    File testDir = this.tempDir.newFolder();
    String filePrefix = "test";
    String filePrefix2 = "second.test";

    SingerLogConfig config = createSingerLogConfig(filePrefix, testDir.getAbsolutePath());
    SingerLogConfig config2 = createSingerLogConfig(filePrefix2, testDir.getAbsolutePath());

    LogStream toMonitor = new LogStream(new SingerLog(config), filePrefix);
    LogStream toMonitor2 = new LogStream(new SingerLog(config2), filePrefix2);

    FileSystemMonitor t = new FileSystemMonitor(Arrays.asList(toMonitor, toMonitor2), "testTwoLogStreamsInSameDir");
    t.start();

    final int NUM_FILES = 10;
    File[] created = new File[NUM_FILES];
    for (int i = 0; i < NUM_FILES; i++) {
      created[i] = File.createTempFile(filePrefix, ".tmp", testDir);
      Thread.sleep(FILE_EVENT_WAIT_TIME_MS);
    }

    final int NUM_FILES_2 = 10;
    File[] created2 = new File[NUM_FILES_2];
    for (int i = 0; i < NUM_FILES_2; i++) {
      created2[i] = File.createTempFile(filePrefix2, ".tmp", testDir);
      Thread.sleep(FILE_EVENT_WAIT_TIME_MS);
    }

    Thread.sleep(15000);
    String[] files = testDir.list();
    assertEquals("Number of files should be the sum of the number of files in the two log streams",
        files.length, toMonitor.size() + toMonitor2.size());

    for (String fileName : testDir.list()) {
      if (toMonitor.containsFile(fileName)) {
        assertFalse("File should be in only one log stream", toMonitor2.containsFile(fileName));
      } else if (toMonitor2.containsFile(fileName)) {
        assertFalse("File should be in only one log stream", toMonitor.containsFile(fileName));
      } else {
        fail("File should be in exactly one log stream");
      }
    }
  }

  @Test
  public void testFilesAlreadyPresent() throws Exception {
    final File testDir = this.tempDir.newFolder();
    final String filePrefix = "test";

    int NUM_FILES = 10;
    File[] created = createTestLogStreamFiles(testDir, "test_001.tmp", NUM_FILES);
    String[] createdFiles = new String[NUM_FILES];
    for (int i = 0; i < NUM_FILES; i++)
      createdFiles[i] = created[i].getName();

    int NUM_FILES_2 = 10;
    File[] created2 = createTestLogStreamFiles(testDir, "test_002.tmp", NUM_FILES_2);
    String[] createdFiles2 = new String[NUM_FILES];
    for (int i = 0; i < NUM_FILES; i++)
      createdFiles2[i] = created2[i].getName();

    SingerLogConfig config = createSingerLogConfig(filePrefix, testDir.getAbsolutePath());
    config.setLogStreamRegex("test_(\\d+).tmp");

    SingerSettings.setSingerConfig(new SingerConfig());
    FileSystemMonitor fs = SingerSettings.getOrCreateFileSystemMonitor("");
    LogStreamManager.initializeLogStreams(new SingerLog(config));

    List<LogStream> logStreams = LogStreamManager.getLogStreamsFor(testDir.toPath(), created[0].toPath());
    assertEquals(1, logStreams.size());
    verifyFiles(createdFiles, logStreams.get(0));

    List<LogStream> logStreams2 = LogStreamManager.getLogStreamsFor(testDir.toPath(), created2[0].toPath());
    assertEquals(1, logStreams2.size());
    verifyFiles(createdFiles2, logStreams2.get(0));
  }


  @Test
  public void testFilesMatchedByMultipleLogStreams() throws Exception {
    final File testDir = this.tempDir.newFolder();
    final String LOG_FILE_PREFIX = "test_001.tmp";
    final String LOGSTREAM_REGEX = "test_(\\d+).tmp";
    final int NUM_CONFIGS = 5;

    int NUM_FILES = 10;
    File[] created = createTestLogStreamFiles(testDir, LOG_FILE_PREFIX, NUM_FILES);
    String[] createdFiles = new String[NUM_FILES];
    for (int i = 0; i < NUM_FILES; i++)
      createdFiles[i] = created[i].getName();

    SingerConfig singerConfig = new SingerConfig();
    List<SingerLogConfig> logStreamConfigs = new ArrayList<>();
    for (int i = 0; i < NUM_CONFIGS; i++) {
      SingerLogConfig config = createSingerLogConfig("logstream_" + i, testDir.getAbsolutePath());
      config.setLogStreamRegex(LOGSTREAM_REGEX);
      logStreamConfigs.add(config);
    }
    singerConfig.setLogConfigs(logStreamConfigs);

    SingerSettings.setSingerConfig(singerConfig);
    FileSystemMonitor fs = SingerSettings.getOrCreateFileSystemMonitor("");
    LogStreamManager.initializeLogStreams();

    List<LogStream> logStreams = LogStreamManager.getLogStreamsFor(testDir.toPath(), created[0].toPath());
    assertEquals(NUM_CONFIGS, logStreams.size());
    for (int i = 0; i < NUM_CONFIGS; i++) {
      verifyFiles(createdFiles, logStreams.get(0));
    }
  }

  @Test
  public void testRenameFile() throws Exception {
    File testDir = this.tempDir.newFolder();

    String filePrefix = "test";

    SingerLogConfig config = createSingerLogConfig(filePrefix, testDir.getAbsolutePath());
    config.setLogStreamRegex("test.tmp");
    LogStream toMonitor = LogStreamManager.createNewLogStream(new SingerLog(config), filePrefix);

    FileSystemMonitor t = new FileSystemMonitor(Arrays.asList(toMonitor), "testRenameFile");
    verifyFiles(testDir.list(), toMonitor);

    int NUM_FILES = 10;
    File[] created = createTestLogStreamFiles(testDir, "test.tmp", NUM_FILES);
    t.start();
    Thread.sleep(FILE_EVENT_WAIT_TIME_MS);

    assertTrue(toMonitor.getLatestLogFileAndPath() != null);
    assertTrue(toMonitor.checkConsistency());
    System.out.println(toMonitor.getLogFilePaths().size());
    assertTrue(toMonitor.getLogFilePaths().size() == NUM_FILES);
    verifyFiles(testDir.list(), toMonitor);

    File toRename = created[1];
    LogFile logFile = new LogFile(SingerUtils.getFileInode(toRename.toPath()));
    assertTrue(!logFile.equals(toMonitor.getLatestLogFileAndPath().getLogFile()));

    LogStream stream2 = LogStreamManager.getLogStreamsFor(testDir.toPath(), toRename.toPath()).get(0);
    assertTrue(stream2 != null);

    String firstFileName = toRename.getName();
    assertTrue(toMonitor.containsFile(firstFileName));
    long firstFileInode = SingerUtils.getFileInode(toRename.toPath());

    String newName = "test.tmp.11";
    toRename.renameTo(new File(testDir, newName));
    Thread.sleep(FILE_EVENT_WAIT_TIME_MS);

    verifyFiles(testDir.list(), toMonitor);

    assertFalse(toMonitor.containsFile(firstFileName));
    assertTrue(toMonitor.containsFile(newName));
    assertEquals(toMonitor.getInodeByFileName(newName), firstFileInode);
  }

  @Test
  public void testAddFiles() throws Exception {
    final File testDir = this.tempDir.newFolder();
    final String filePrefix = "test";

    SingerLogConfig config = createSingerLogConfig(filePrefix, testDir.getAbsolutePath());
    LogStream toMonitor = new LogStream(new SingerLog(config), filePrefix);
    FileSystemMonitor t = new FileSystemMonitor(Arrays.asList(toMonitor), "testAddFiles");
    verifyFiles(testDir.list(), toMonitor);

    int NUM_FILES = 10;
    File[] created = new File[NUM_FILES];
    for (int i = 0; i < NUM_FILES; i++) {
      created[i] = File.createTempFile(filePrefix, "", testDir);
    }
    t.start();
    Thread.sleep(FILE_EVENT_WAIT_TIME_MS);
    verifyFiles(testDir.list(), toMonitor);
  }

  @Test
  public void testRemoveFiles() throws Exception {
    final File testDir = this.tempDir.newFolder();
    final String filePrefix = "test.tmp";

    SingerLogConfig config = createSingerLogConfig(filePrefix, testDir.getAbsolutePath());
    config.setLogStreamRegex("test.tmp");

    LogStream toMonitor = LogStreamManager.createNewLogStream(new SingerLog(config), filePrefix);
    FileSystemMonitor t = new FileSystemMonitor(Arrays.asList(toMonitor), "testRemoveFiles");
    verifyFiles(testDir.list(), toMonitor);

    final int NUM_FILES = 10;
    File[] created = createTestLogStreamFiles(testDir, "test.tmp", NUM_FILES);
    t.start();
    Thread.sleep(FILE_EVENT_WAIT_TIME_MS);
    verifyFiles(testDir.list(), toMonitor);

    int FIRST_HALF = NUM_FILES / 2;
    for (int i = 1; i < FIRST_HALF + 1; i++) {
      created[i].delete();
    }

    Thread.sleep(FILE_EVENT_WAIT_TIME_MS);
    verifyFiles(testDir.list(), toMonitor);

    for (int i = 1; i < FIRST_HALF + 1; i++) {
      String s = created[i].getName();
      assertFalse("deleted files shouldn't be in there",  toMonitor.containsFile(s));
    }
  }


  @Test
  public void testWatchMultipleDirectories() throws Exception {
    final File testDir = this.tempDir.newFolder();
    final String filePrefix = "test";
    final File testDir2 = this.tempDir.newFolder();
    final String filePrefix2 = "2ndTest";

    SingerLogConfig config = createSingerLogConfig(filePrefix, testDir.getAbsolutePath());
    SingerLogConfig config2 = createSingerLogConfig(filePrefix2, testDir2.getAbsolutePath());
    config.setLogStreamRegex("test.tmp");
    config2.setLogStreamRegex("2ndTest.tmp");

    LogStream toMonitor = new LogStream(new SingerLog(config), filePrefix);
    LogStream toMonitor2 = new LogStream(new SingerLog(config2), filePrefix2);

    FileSystemMonitor t = new FileSystemMonitor(Arrays.asList(toMonitor, toMonitor2), "testWatchMultipleDirectories");
    t.start();
    Thread.sleep(FILE_EVENT_WAIT_TIME_MS);
    verifyFiles(testDir.list(), toMonitor);
    verifyFiles(testDir2.list(), toMonitor2);

    int NUM_FILES = 10;
    File[] created = createTestLogStreamFiles(testDir, "test.tmp", NUM_FILES);
    Thread.sleep(FILE_EVENT_WAIT_TIME_MS);

    verifyFiles(testDir.list(), toMonitor);
    verifyFiles(testDir2.list(), toMonitor2);

    int NUM_FILES_2 = 20;
    File[] created2 = createTestLogStreamFiles(testDir2, "2ndTest.tmp", NUM_FILES_2);

    Thread.sleep(FILE_EVENT_WAIT_TIME_MS);

    verifyFiles(testDir.list(), toMonitor);
    verifyFiles(testDir2.list(), toMonitor2);

    for (int i = 0; i < NUM_FILES_2 / 2; i++) {
      created2[i].delete();
    }
    Thread.sleep(FILE_EVENT_WAIT_TIME_MS);
    verifyFiles(testDir.list(), toMonitor);
    verifyFiles(testDir2.list(), toMonitor2);

    for (int i = 1; i < NUM_FILES; i++) {
      created[i].delete();
    }
    Thread.sleep(FILE_EVENT_WAIT_TIME_MS);
    verifyFiles(testDir.list(), toMonitor);
    verifyFiles(testDir2.list(), toMonitor2);
  }

  @Test
  public void testLogFileRotation() throws Exception {
    final File testDir = this.tempDir.newFolder();
    final String filePrefix = "test";

    SingerLogConfig config = createSingerLogConfig(filePrefix, testDir.getAbsolutePath());
    config.setLogStreamRegex("test.tmp");

    LogStream toMonitor = new LogStream(new SingerLog(config), filePrefix);
    FileSystemMonitor t = new FileSystemMonitor(Arrays.asList(toMonitor), "testLogFileRotation");
    t.start();
    Thread.sleep(FILE_EVENT_WAIT_TIME_MS);

    int NUM_FILES = 10;
    File[] created = createTestLogStreamFiles(testDir, "test.tmp", NUM_FILES);

    Thread.sleep(FILE_EVENT_WAIT_TIME_MS);

    File[] renamed = new File[ created.length + 1];
    created[0].delete();

    for (int i = 1; i < NUM_FILES; i++) {
      renamed[i] = new File(testDir, "test.tmp." + (NUM_FILES - i));
      created[i].renameTo(renamed[i]);
      Thread.sleep(FILE_EVENT_WAIT_TIME_MS);
      toMonitor.logStatus();
      assertTrue(toMonitor.checkConsistency());
    }
    renamed[NUM_FILES]  = new File(testDir, filePrefix);
    renamed[NUM_FILES].createNewFile();

    Thread.sleep(FILE_EVENT_WAIT_TIME_MS);
    toMonitor.logStatus();
    assertTrue(toMonitor.checkConsistency());
  } 
}