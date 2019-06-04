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

import com.pinterest.singer.SingerTestBase;
import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerLog;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.LogMessageAndPosition;
import com.pinterest.singer.thrift.configuration.FileNameMatchMode;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;

import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;
import org.junit.Test;

import java.io.File;
import java.util.List;

public class LogFileUtilsTest extends SingerTestBase {

  /**
   * Create a list of files under /tmp/junit___/thrift.log*
   */
  private void createLogFiles (String path) throws Exception {
    SimpleThriftLogger<LogMessage> logger = new SimpleThriftLogger<>(path);
    int numOfLogRotations = 4;
    int numOfLogMessagesPerLogFile = 100;
    List<LogMessageAndPosition> messagesWritten = Lists.newArrayListWithExpectedSize
        (numOfLogRotations * numOfLogMessagesPerLogFile);

    try {
      writeThriftLogMessages(logger, 150, 500, 50);
      for (int i = 0; i < numOfLogRotations - 1; ++i) {
        rotateWithDelay(logger, 1000);
        messagesWritten.addAll(writeThriftLogMessages(logger, numOfLogMessagesPerLogFile, 500, 50));
      }
    } finally {
      logger.close();
    }
  }

  @Test
  public void testGetFilePathByInode() throws Exception {
    String logFilePath = FilenameUtils.concat(getTempPath(), "thrift.log");
    SingerLog singerLog = new SingerLog(
        new SingerLogConfig("test", getTempPath(), "thrift.log", null, null, null));

    singerLog.getSingerLogConfig().setFilenameMatchMode(FileNameMatchMode.PREFIX);
    LogStream logStream = new LogStream(singerLog, "thrift.log");
    createLogFiles(logFilePath);

    File logDir = new File(getTempPath());
    File[] logFiles = logDir.listFiles();
    for (File file : logFiles) {
      if (file.isDirectory()) continue;
      long inode = SingerUtils.getFileInode(file.toPath());
      String path = LogFileUtils.getFilePathByInode(logStream, inode);
      assertEquals(path, file.getAbsolutePath());
    }
  }
}
