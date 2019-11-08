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
package com.pinterest.singer.reader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.junit.Test;

import com.pinterest.singer.SingerTestBase;
import com.pinterest.singer.thrift.LogFile;
import com.pinterest.singer.thrift.LogMessageAndPosition;
import com.pinterest.singer.thrift.configuration.TextLogMessageType;
import com.pinterest.singer.utils.SingerUtils;
import com.pinterest.singer.utils.TextLogger;

public class TestTextLogFileReader extends SingerTestBase {

  @Test
  public void testReadLogMessageAndPosition() throws Exception {
    String path = FilenameUtils.concat(getTempPath(), "test2.log");
    List<String> dataWritten = generateDummyMessagesToFile(path);

    long inode = SingerUtils.getFileInode(SingerUtils.getPath(path));
    LogFile logFile = new LogFile(inode);
    LogFileReader reader = new TextLogFileReader(logFile, path, 0, 8192, 102400, 1,
        Pattern.compile("^.*$"), TextLogMessageType.PLAIN_TEXT, false, false, null, null);
    for (int i = 0; i < 100; i++) {
      LogMessageAndPosition log = reader.readLogMessageAndPosition();
      assertEquals(dataWritten.get(i), new String(log.getLogMessage().getMessage()));
    }
    reader.close();
  }
  
  @Test
  public void testReadLogMessageAndPositionWithHostname() throws Exception {
    String path = FilenameUtils.concat(getTempPath(), "test2.log");
    List<String> dataWritten = generateDummyMessagesToFile(path);
    String delimiter = " ";
    String hostname = "test";

    long inode = SingerUtils.getFileInode(SingerUtils.getPath(path));
    LogFile logFile = new LogFile(inode);
    LogFileReader reader = new TextLogFileReader(logFile, path, 0, 8192, 102400, 1,
        Pattern.compile("^.*$"), TextLogMessageType.PLAIN_TEXT, false, true, hostname, delimiter);
    for (int i = 0; i < 100; i++) {
      LogMessageAndPosition log = reader.readLogMessageAndPosition();
      assertEquals(hostname + delimiter + dataWritten.get(i), new String(log.getLogMessage().getMessage()));
    }
    reader.close();
  }
  
  @Test
  public void testReadLogMessageAndPositionMultiRead() throws Exception {
    String path = FilenameUtils.concat(getTempPath(), "test2.log");
    List<String> dataWritten = generateDummyMessagesToFile(path);

    long inode = SingerUtils.getFileInode(SingerUtils.getPath(path));
    LogFile logFile = new LogFile(inode);
    LogFileReader reader = new TextLogFileReader(logFile, path, 0, 8192, 102400, 2,
        Pattern.compile("^.*$"), TextLogMessageType.PLAIN_TEXT, false, false, null, null);
    for (int i = 0; i < 100; i = i + 2) {
      LogMessageAndPosition log = reader.readLogMessageAndPosition();
      assertEquals(dataWritten.get(i) + dataWritten.get(i + 1),
          new String(log.getLogMessage().getMessage()));
    }
    assertNull(reader.readLogMessageAndPosition());
    reader.close();
  }

  private List<String> generateDummyMessagesToFile(String path) throws FileNotFoundException, IOException {
    TextLogger logger = new TextLogger(path);
    List<String> dataWritten = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      StringBuilder builder = new StringBuilder();
      for (int j = 0; j < ThreadLocalRandom.current().nextInt(10, 100); j++) {
        builder.append(UUID.randomUUID().toString());
      }
      builder.append('\n');
      String str = builder.toString();
      dataWritten.add(str);
      logger.logText(str);
    }
    return dataWritten;
  }
}