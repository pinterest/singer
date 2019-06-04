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
import com.pinterest.singer.thrift.LogFile;
import com.pinterest.singer.thrift.LogFileAndPath;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;

import com.pinterest.singer.utils.SingerUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LogDirectoryScannerTest extends com.pinterest.singer.SingerTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(LogDirectoryScannerTest.class);
  private static final String filePrefix = "test.tmp";
  private static final int NUM_FILES = 10;

  private LogStream initializeTestLogStream(File testDir, String filePrefix) throws Exception {
    System.out.println("testDir: " + testDir.getPath());
    SingerLogConfig singerLogConfig = createSingerLogConfig("test", testDir.getPath());
    SingerLog singerLog = new SingerLog(singerLogConfig);
    LogStream logStream = LogStreamManager.createNewLogStream(singerLog, filePrefix);
    return logStream;
  }

  private File[] createTestLogFiles(File testDir, String filePrefix, int numFiles) throws Exception {
    // Generate test.tmp.9, test.tmp.8, test.tmp.7, ... test.tmp.1, test.tmp
    File[] files = createTestLogStreamFiles(testDir, filePrefix, numFiles);
    List<LogStream> logStreams = LogStreamManager.getLogStreamsFor(testDir.toPath(), files[0].toPath());
    LOG.info("# of files in log stream : {}", logStreams.get(0).getLogFileAndPaths().size());
    assertEquals(numFiles, logStreams.get(0).getLogFileAndPaths().size());
    return files;
  }

  private List<File> rotateFile(File testDir, String filePrefix, int numFiles, File[] files) throws Exception {
    ArrayList<File> newFiles = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      File renamed = new File(testDir, filePrefix + "." + (numFiles - i));
      boolean success = files[i].renameTo(renamed);
      if (!success) {
        fail("Failed to rename file ");
      }
      newFiles.add(renamed);
      Thread.sleep(FILE_EVENT_WAIT_TIME_MS);
    }
    return newFiles;
  }

  /***
   * Test the LogDirectoryScanner functionality, and make sure that the invariants of LogStream
   * are maintains after LogDirectoryScanner updates [inode -> path] mapping.
   */
  @Test
  public void testLogDirectoryScanner() throws Exception {
    final File testDir = this.tempDir.newFolder();

    try {
      LogStream toMonitor = initializeTestLogStream(testDir, filePrefix);

      FileSystemMonitor fileSystemMonitor = new FileSystemMonitor(Arrays.asList(toMonitor), "testLogDirectoryScanner");
      fileSystemMonitor.start();

      File[] files = createTestLogFiles(testDir, filePrefix, NUM_FILES);

      Thread.sleep(10000);
      List<LogFileAndPath> logFileAndPaths = toMonitor.getLogFileAndPaths();

      // rotate files
      List<File> rotatedFiles = rotateFile(testDir, filePrefix, NUM_FILES, files);

      File newHeader = new File(testDir, filePrefix);
      if (!newHeader.createNewFile()) {
        fail("Failed to create the stream header file");
      }
      Thread.sleep(50);
      Set<Path> monitoredPaths = new HashSet<>();
      monitoredPaths.add(testDir.toPath());
      
      LogDirectoriesScanner scanner = new LogDirectoriesScanner(monitoredPaths);
      scanner.start();
      Thread.sleep(10000);

      LogStream stream = LogStreamManager.getLogStreamsFor(testDir.toPath(), files[0].toPath()).get(0);
      List<LogFileAndPath> newLogFileAndPaths = stream.getLogFileAndPaths();
      logFileAndPaths.stream().forEachOrdered(e -> LOG.info("{}", e));
      LOG.info("New log files and paths:");
      newLogFileAndPaths.stream().forEachOrdered(e -> LOG.info("{}", e));
      for (int i = 0; i < NUM_FILES; i++) {
        assertEquals(logFileAndPaths.get(i).getLogFile(), newLogFileAndPaths.get(i).getLogFile());
        assertEquals("", logFileAndPaths.get(i).getPath(), newLogFileAndPaths.get(i + 1).getPath());
      }
      scanner.join();
    } catch(Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   *  Test file deletion in the middle of log directory scan.
   *
   */
  @Test
  public void testLogDirectoryScannerWithFileDeletion() throws Exception {
    final File testDir = this.tempDir.newFolder();
    try {
      LogStream toMonitor = initializeTestLogStream(testDir, filePrefix);
      FileSystemMonitor fileSystemMonitor =
          new FileSystemMonitor(Arrays.asList(toMonitor), "testLogDirectoryScannerWithFileDeletion");
      fileSystemMonitor.start();

      File[] files = createTestLogFiles(testDir, filePrefix, NUM_FILES);
      LogStream stream = LogStreamManager.getLogStreamsFor(testDir.toPath(), files[0].toPath()).get(0);
      LOG.info("stream.getLogFileAndPaths().size() = {}", stream.getLogFileAndPaths().size());
      assertEquals(NUM_FILES, stream.getLogFileAndPaths().size());

      Thread.sleep(4000);
      List<LogFileAndPath> logFileAndPaths = toMonitor.getLogFileAndPaths();
      // rotate files
      List<File> rotatedFiles = rotateFile(testDir, filePrefix, NUM_FILES, files);

      File newHeader = new File(testDir, filePrefix);
      if (!newHeader.createNewFile()) {
        fail("Failed to create the stream header file");
      }
      Thread.sleep(50);

      Set<Path> monitoredPaths = new HashSet<>();
      monitoredPaths.add(testDir.toPath());

      List<LogFileAndPath> newLogFileAndPaths = stream.getLogFileAndPaths();
      newLogFileAndPaths.stream().forEachOrdered(e -> LOG.info("{}", e));

      LogDirectoriesScanner scanner = new LogDirectoriesScanner(monitoredPaths);

      // execute LogDirectoriesScanner related methods
      Path dirPath = scanner.getMonitoredPaths().get(0);
      List<File> sortedFiles = scanner.getFilesInSortedOrder(dirPath);
      sortedFiles.stream().forEachOrdered(file -> System.out.println(file.getPath()));

      sortedFiles.stream().forEachOrdered( file -> LOG.info("{}", file));

      LogFile deletedLogFile = new LogFile(SingerUtils.getFileLastModifiedTime(sortedFiles.get(0)));
      String deletedPath = sortedFiles.get(0).getPath();
      sortedFiles.get(0).delete();

      Thread.sleep(2000);
      scanner.updateFilesInfo(dirPath, sortedFiles);

      stream.put(deletedLogFile, deletedPath);

      // the deleted file is still recorded in the log stream
      assertTrue(stream.containsFile(sortedFiles.get(0).getName()));

      // verify that after adding the deleted file, the files in logstream are still ordered
      assertTrue(stream.checkConsistency());
    } catch(Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
}