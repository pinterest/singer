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
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.LogMessageAndPosition;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.utils.SimpleThriftLogger;

import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;
import org.apache.kafka.common.utils.ByteUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class ThriftLogFileReaderTest extends SingerTestBase {

  @Test
  public void testReadBadMessage() throws Exception {
    String path = FilenameUtils.concat(getTempPath(), "thrift.log");

    SimpleThriftLogger<LogMessage> logger = new SimpleThriftLogger<>(path);

    try {
      // Write messages to be skipped.
      writeThriftLogMessages(logger, 200, 1, 50);
      writeThriftLogMessages(logger, 3, 1, 4980);
      writeThriftLogMessages(logger, 200, 1, 50);
    } finally {
      logger.close();
    }

    LogStream logStream = new LogStream(new SingerLog(new SingerLogConfig()), "test");

    // Open reader which cap the log message at 500 bytes
    LogFileReader reader = new ThriftLogFileReader(logStream, logger.getLogFile(), path, 0L, 16000, 500, null);
    int count = 0;
    for (int i = 0; i < 403; i++) {
      try {
        // Seek to start offset.
        reader.readLogMessageAndPosition();
        count++;
      } catch (LogFileReaderException exception) {
        // Ignore the exception.
      }
    }
    assertEquals(403, count);
    reader.close();
  }

  @Test
  public void testReadLogMessageAndPosition() throws Exception {
    String path = FilenameUtils.concat(getTempPath(), "thrift.log");

    SimpleThriftLogger<LogMessage> logger = new SimpleThriftLogger<>(path);

    long startOffset = 0L;
    List<LogMessageAndPosition> messagesWritten = null;
    try {
      // Write messages to be skipped.
      writeThriftLogMessages(logger, 200, 500, 50);

      startOffset = logger.getByteOffset();

      messagesWritten = writeThriftLogMessages(logger, 3, 500, 5000);
    } finally {
      logger.close();
    }

    LogStream logStream = new LogStream(new SingerLog(new SingerLogConfig()), "test");

    // Open reader which cap the log message at 500 bytes
    LogFileReader reader = new ThriftLogFileReader(logStream, logger.getLogFile(), path, 0L, 16000, 500, null);
    try {
      // Seek to start offset.
      reader.setByteOffset(startOffset);
      // Read one log message (500 message + 50 message key) which is bigger than the
      // max message
      // size.
      reader.readLogMessageAndPosition();
      fail("Should throw when thrift message is bigger than max message size");
    } catch (LogFileReaderException exception) {
      // Ignore the exception.
    } finally {
      reader.close();
    }

    // Open reader.
    reader = new ThriftLogFileReader(logStream, logger.getLogFile(), path, 0L, 16000, 16000, null);
    List<LogMessageAndPosition> messagesRead = Lists.newArrayListWithExpectedSize(3);
    try {
      // Seek to start offset.
      reader.setByteOffset(startOffset);
      // Read log file until no more messages.
      LogMessageAndPosition message = reader.readLogMessageAndPosition();
      while (message != null) {
        messagesRead.add(message);
        message = reader.readLogMessageAndPosition();
      }
    } finally {
      reader.close();
    }

    // Return null when no more message in the log file.
    assertThat(messagesRead, is(messagesWritten));
  }

  public void testEnvironmentVariableInjection() throws Exception {
    String path = FilenameUtils.concat(getTempPath(), "thrift.log");
    SimpleThriftLogger<LogMessage> logger = new SimpleThriftLogger<>(path);
    List<LogMessageAndPosition> messagesWritten;
    try {
      messagesWritten = writeThriftLogMessages(logger, 3, 500, 5000);
    } finally {
      logger.close();
    }

    LogStream logStream = new LogStream(new SingerLog(new SingerLogConfig()), "test");

    // Open reader.
    ThriftLogFileReader reader = new ThriftLogFileReader(logStream, logger.getLogFile(), path, 0L, 16000, 16000,
        Collections.singletonMap("test", ByteBuffer.wrap("test_value".getBytes())));
    List<LogMessageAndPosition> messagesRead = Lists.newArrayListWithExpectedSize(3);
    try {
      // Read log file until no more messages.
      LogMessageAndPosition message = reader.readLogMessageAndPosition();
      while (message != null) {
        messagesRead.add(message);
        assertNotNull(message.getInjectedHeaders());
        assertEquals(1, message.getInjectedHeaders().size());
        assertTrue(message.getInjectedHeaders().containsKey("test"));
        assertTrue(Arrays.equals("test_value".getBytes(), message.getInjectedHeaders().get("test").array()));
        message = reader.readLogMessageAndPosition();
      }
    } finally {
      reader.close();
    }

    // Return null when no more message in the log file.
    assertEquals(
        messagesRead.stream().map(LogMessageAndPosition::getLogMessage).collect(Collectors.toList()),
        messagesWritten.stream().map(LogMessageAndPosition::getLogMessage).collect(Collectors.toList())
    );
  }
}