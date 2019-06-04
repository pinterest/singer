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
package com.pinterest.singer.common;

import com.pinterest.singer.thrift.LogFile;
import com.pinterest.singer.thrift.LogFileAndPath;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.utils.SingerUtils;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

public class LogStreamTest extends com.pinterest.singer.SingerTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(LogStreamTest.class);

  @Test
  public void testLogStream() throws Exception {
    final File testDir = this.tempDir.newFolder();
    final String filePrefix = "test.tmp";
    final int NUM_FILES = 10;

    SingerLogConfig singerLogConfig = createSingerLogConfig("test", testDir.getPath());
    SingerLog singerLog = new SingerLog(singerLogConfig);

    // generate log file sequence [test.tmp.9, test.tmp.8, ..., test.tmp.1, test.tmp]
    // here the files are in descending order by the last modification time.
    File[] files = createTestLogStreamFiles(testDir, filePrefix, NUM_FILES);
    LogStream stream = new LogStream(singerLog, filePrefix);
    stream.initialize();
    assertEquals("", stream.getLogFileAndPaths().size(), NUM_FILES);

    List<LogFileAndPath> logFileAndPaths = stream.getLogFileAndPaths();
    logFileAndPaths.stream().forEachOrdered(lfp -> LOG.info("{}", lfp));

    for (int i = 0; i < NUM_FILES; i++) {
      LogFileAndPath logFileAndPath = logFileAndPaths.get(i);
      assertEquals(logFileAndPath.getPath(), files[i].toString());
      assertEquals(logFileAndPath.getLogFile().inode, SingerUtils.getFileInode(files[i].toPath()));
      if (i < NUM_FILES - 1) {
        assertEquals(stream.getNext(logFileAndPath.getLogFile()).getLogFile().inode,
            SingerUtils.getFileInode(files[i+1].toPath()));
      }
      if (i > 0) {
        assertEquals(stream.getPrev(logFileAndPath.getLogFile()).getLogFile().inode,
            SingerUtils.getFileInode(files[i-1].toPath()));
      }
    }
    assertTrue(stream.checkConsistency());
  }

  @Test
  public void testLogStreamUpdate() throws Exception {
    final File testDir = this.tempDir.newFolder();
    final String filePrefix = "test.tmp";

    SingerLogConfig singerLogConfig = createSingerLogConfig("test", testDir.getPath());
    SingerLog singerLog = new SingerLog(singerLogConfig);
    LogStream logStream = new LogStream(singerLog, filePrefix);

    // As these "test.*" files do not exist on local disk, their lastModifiedTime will be 0
    // Because of that, logStream will sort them in the sort of
    //  "test.3,  test.2,  test.1"
    logStream.append(new LogFile(1L), "test.1");
    logStream.append(new LogFile(2L), "test.2");
    logStream.append(new LogFile(3L), "test.3");

    assertEquals(logStream.getLogFileAndPaths().size(), 3);
    assertEquals(logStream.getNext(new LogFile(3L)).getLogFile().getInode(), 2L);
    assertEquals(logStream.getNext(new LogFile(2L)).getLogFile().getInode(), 1L);
    assertEquals(logStream.getNext(new LogFile(1L)), null);

    logStream.put(new LogFile(2L), "test.4");
    assertEquals(logStream.getLogFileAndPaths().size(), 3);

    logStream.put(new LogFile(1L), "test.5");
    assertEquals(logStream.getLogFileAndPaths().size(), 3);

    logStream.put(new LogFile(1L), "test.5");
    assertEquals(logStream.getLogFileAndPaths().size(), 3);

    logStream.removeLogFilePathInfo("test.1");
    assertEquals(logStream.getLogFileAndPaths().size(), 3);

    logStream.removeLogFilePathInfo("test.5");
    assertEquals(logStream.getLogFileAndPaths().size(), 2);

    List<LogFileAndPath> logFileAndPaths = logStream.getLogFileAndPaths();
    for (LogFileAndPath logFileAndPath : logFileAndPaths) {
      String path = logStream.getLogFilePath(logFileAndPath.getLogFile());
      assertEquals(path, logFileAndPath.getPath());
    }

  }
}
