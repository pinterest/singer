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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import com.pinterest.singer.SingerTestBase;
import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerLog;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.monitor.LogStreamManager;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.LogMessageAndPosition;
import com.pinterest.singer.thrift.LogPosition;
import com.pinterest.singer.thrift.configuration.FileNameMatchMode;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.thrift.configuration.ThriftReaderConfig;
import com.pinterest.singer.utils.SimpleThriftLogger;

import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;
import org.junit.Test;

import java.io.File;
import java.util.List;

public class DefaultLogStreamReaderTest extends SingerTestBase {

  @Test
  public void testReadLogMessageAndPosition() throws Exception {
    String path = FilenameUtils.concat(getTempPath(), "thrift2.log");
    String tempPath = getTempPath();

    SingerLogConfig singerLogConfig = new SingerLogConfig("test", tempPath, "thrift2.log", null, null, null);
    singerLogConfig.setFilenameMatchMode(FileNameMatchMode.PREFIX);
    SingerLog singerLog = new SingerLog(singerLogConfig);

    SingerSettings.setSingerConfig(new SingerConfig());
    SingerSettings.getOrCreateFileSystemMonitor("").start();
    SingerSettings.getOrCreateFileSystemMonitor("").registerPath(new File(tempPath).toPath());

    LogStream logStream = new LogStream(singerLog, "thrift2.log");
    LogStreamManager.addLogStream(logStream);

    SimpleThriftLogger<LogMessage> logger = new SimpleThriftLogger<>(path);
    LogPosition startPosition = null;

    int numOfLogRotations = 4;
    int numOfLogMessagesPerLogFile = 300;
    List<LogMessageAndPosition> messagesWritten = Lists
        .newArrayListWithExpectedSize(numOfLogRotations * numOfLogMessagesPerLogFile);

    try {
      // Write messages to be skipped.
      writeThriftLogMessages(logger, 150, 500, 50);

      // Start position to read from.
      startPosition = new LogPosition(logger.getLogFile(), logger.getByteOffset());

      // Rotate log file while writing messages.
      for (int i = 0; i < numOfLogRotations - 1; ++i) {
        rotateWithDelay(logger, 1000);
        messagesWritten.addAll(writeThriftLogMessages(logger, numOfLogMessagesPerLogFile, 500, 50));
      }

      // Write one message which exceeds the maximum message size, followed by 10 valid messages.
      // The first thrift message will be ignored. But the followed 10 messages will be read as
      // Singer internally tolerates messages that is 10 time of the maxMessageSize.
      writeThriftLogMessages(logger, 1, 500, 16001);
      messagesWritten.addAll(writeThriftLogMessages(logger, 10, 500, 500));

      rotateWithDelay(logger, 1000);
      messagesWritten.addAll(writeThriftLogMessages(logger, numOfLogMessagesPerLogFile, 500, 50));

      // Write one message whose size is more than 10 time of the message size limit. Singer log
      // reader will ignore this message and all messages that follows it. The latter setting
      // for ThriftReaderConfig sets the max size of message to 16K, hence Singer will only
      // be able to tolerate messages that are smaller or equals to 160K.
      writeThriftLogMessages(logger, 1, 500, 160001);
      writeThriftLogMessages(logger, 10, 500, 500);
    } finally {
      logger.close();
    }

    System.err.println("Waiting for file system events to be noticed by FSM");
    while(logStream.isEmpty()) {
  	  Thread.sleep(1000);
  	  System.out.print(".");
    }

    DefaultLogStreamReader reader = new DefaultLogStreamReader(logStream,
        new ThriftLogFileReaderFactory(new ThriftReaderConfig(16000, 16000)));

    List<LogMessageAndPosition> messagesRead = Lists.newArrayListWithExpectedSize(
        numOfLogRotations * numOfLogMessagesPerLogFile);
    try {
      // Seek to start position.
      reader.seek(startPosition);
      // Read all log files in rotation sequence until no more messages.
      LogMessageAndPosition message = reader.readLogMessageAndPosition();
      while (message != null) {
        messagesRead.add(message);
        message = reader.readLogMessageAndPosition();
      }
    } finally {
      reader.close();
    }

    System.out.println(
        "# messages read = " + messagesRead.size() + "  # messages written = " + messagesWritten
            .size());
    assertThat(String.format("Wrote %d messages, read %d.", messagesWritten.size(), messagesRead.size()),
            messagesRead, is(messagesWritten));
  }
}