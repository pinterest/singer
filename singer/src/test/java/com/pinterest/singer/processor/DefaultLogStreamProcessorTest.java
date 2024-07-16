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
package com.pinterest.singer.processor;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.LogStreamReader;
import com.pinterest.singer.common.errors.LogStreamProcessorException;
import com.pinterest.singer.common.LogStreamWriter;
import com.pinterest.singer.common.errors.LogStreamWriterException;
import com.pinterest.singer.common.SingerLog;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.config.Decider;
import com.pinterest.singer.monitor.LogStreamManager;
import com.pinterest.singer.reader.DefaultLogStreamReader;
import com.pinterest.singer.reader.ThriftLogFileReaderFactory;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.LogMessageAndPosition;
import com.pinterest.singer.thrift.LogPosition;
import com.pinterest.singer.thrift.configuration.FileNameMatchMode;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.thrift.configuration.ThriftReaderConfig;
import com.pinterest.singer.utils.SimpleThriftLogger;
import com.pinterest.singer.utils.SingerUtils;
import com.pinterest.singer.utils.WatermarkUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultLogStreamProcessorTest extends com.pinterest.singer.SingerTestBase {

  LogStreamReader logStreamReader;
  DefaultLogStreamProcessor processor;
  NoOpLogStreamWriter writer;

  /**
   * No-op implementation of LogStreamWriter which collect all LogMessages in a list.
   */
  private static final class NoOpLogStreamWriter implements LogStreamWriter {

    private final List<LogMessage> logMessages;

    private boolean throwOnWrite;

    public NoOpLogStreamWriter() {
      logMessages = Lists.newArrayList();
      throwOnWrite = false;
    }

    @Override
    public LogStream getLogStream() {
      return null;
    }

    @Override
    public boolean isAuditingEnabled() {
      return false;
    }

    @Override
    public void writeLogMessages(List<LogMessage> logMessages) throws LogStreamWriterException {
      if (throwOnWrite) {
        throw new LogStreamWriterException("Write error");
      } else {
        this.logMessages.addAll(logMessages);
      }
    }

    @Override
    public void close() throws IOException {
    }

    public List<LogMessage> getLogMessages() {
      return logMessages;
    }

    public void setThrowOnWrite(boolean throwOnWrite) {
      this.throwOnWrite = throwOnWrite;
    }
  }

  private SingerConfig initializeSingerConfig(int processorThreadPoolSize, int writerThreadPoolSize,
      List<SingerLogConfig> singerLogConfigs) {
    SingerConfig singerConfig = new SingerConfig();
    singerConfig.setThreadPoolSize(1);
    singerConfig.setWriterThreadPoolSize(1);
    singerConfig.setLogConfigs(singerLogConfigs);
    return singerConfig;
  }

  private void initializeReaderAndProcessor(Map<String, String> overrides, LogStream logStream) {
    Map<String, String> propertyMap = new HashMap<>();
    propertyMap.put("processorBatchSize", "50");
    propertyMap.put("processingIntervalInMillisMin", "1");
    propertyMap.put("processingIntervalInMillisMax", "1");
    propertyMap.put("processingTimeSliceInMilliseconds", "3600");
    propertyMap.put("logRetentionInSecs", "15");
    propertyMap.put("readerBufferSize", "16000");
    propertyMap.put("maxMessageSize", "16000");
    propertyMap.put("logDecider", null);

    if (overrides != null) {
      propertyMap.putAll(overrides);
    }

    logStreamReader = new DefaultLogStreamReader(
        logStream, new ThriftLogFileReaderFactory(
        new ThriftReaderConfig(Integer.valueOf(propertyMap.get("readerBufferSize")),
            Integer.valueOf(propertyMap.get("maxMessageSize")))));
    processor = new DefaultLogStreamProcessor(logStream, propertyMap.get("logDecider"),
        logStreamReader, writer, Integer.valueOf(propertyMap.get("processorBatchSize")),
        Integer.valueOf(propertyMap.get("processingIntervalInMillisMin")),
        Integer.valueOf(propertyMap.get("processingIntervalInMillisMax")),
        Integer.valueOf(propertyMap.get("processingTimeSliceInMilliseconds")),
        Integer.valueOf(propertyMap.get("logRetentionInSecs")));
  }

  @After
  public void cleanup() throws IOException {
    if (processor != null) {
      processor.close();
    }
    writer = null;
    processor = null;
    logStreamReader = null;
  }

  @Test
  public void testProcessKeyedLogStream() throws Exception {
    testProcessLogStream(true);
  }

  @Test
  public void testProcessNonKeyedLogStream() throws Exception {
    testProcessLogStream(false);
  }

  public void testProcessLogStream(boolean isKeyed) throws Exception {
    String tempPath = getTempPath();
    String logStreamHeadFileName = "thrift.log";
    String path = FilenameUtils.concat(tempPath, logStreamHeadFileName);

    int oldestThriftLogIndex = 0;
    int processorBatchSize = 50;

    // initialize a singer log config
    SingerLogConfig logConfig = new SingerLogConfig("test", tempPath, logStreamHeadFileName, null, null, null);
    SingerLog singerLog = new SingerLog(logConfig);
    singerLog.getSingerLogConfig().setFilenameMatchMode(FileNameMatchMode.PREFIX);

    // initialize global variables in SingerSettings
    try {
      SingerConfig singerConfig = initializeSingerConfig(1, 1, Arrays.asList(logConfig));
      SingerSettings.initialize(singerConfig);
    } catch (Exception e) {
      e.printStackTrace();
      fail("got exception in test: " + e);
    }

    // initialize log stream
    LogStream logStream = new LogStream(singerLog, logStreamHeadFileName);
    LogStreamManager.addLogStream(logStream);
    SimpleThriftLogger<LogMessage> logger = new SimpleThriftLogger<>(path);
    writer = new NoOpLogStreamWriter();

    // initialize reader, writer & processor
    initializeReaderAndProcessor(Collections.singletonMap("processorBatchSize", String.valueOf(processorBatchSize)), logStream);

    try {
      // Write messages to be skipped.
      if (isKeyed)
        writeThriftLogMessages(logger, 150, 500, 50);
      else
        writeThriftLogMessages(logger, 150, 50);

      // Save start position to watermark file.
      LogPosition startPosition = new LogPosition(logger.getLogFile(), logger.getByteOffset());
      WatermarkUtils.saveCommittedPositionToWatermark(DefaultLogStreamProcessor
              .getWatermarkFilename(logStream), startPosition);

      List<LogMessage> messagesWritten = Lists.newArrayList();

      // Rotate log file while writing messages.
      for (int i = 0; i < 3; ++i) {
        rotateWithDelay(logger, 1000);
        List<LogMessageAndPosition> logMessageAndPositions = isKeyed ?
                writeThriftLogMessages(logger, processorBatchSize + 20, 500, 50) :
                writeThriftLogMessages(logger, processorBatchSize + 20, 50);
        List<LogMessage> logMessages = getMessages(logMessageAndPositions);
        messagesWritten.addAll(logMessages);
      }

      waitForFileSystemEvents(logStream);

      // Process all message written so far.
      long numOfMessageProcessed = processor.processLogStream();
      assertEquals("Should have processed all messages written", messagesWritten.size(),
              numOfMessageProcessed);
      assertThat(writer.getLogMessages(), is(messagesWritten));

      // Write and process a single LogMessages.
      messagesWritten.addAll(getMessages(isKeyed ?
              writeThriftLogMessages(logger, 1, 500, 50) :
              writeThriftLogMessages(logger, 1, 50))
      );
      numOfMessageProcessed = processor.processLogStream();
      assertEquals("Should have processed a single log message", 1, numOfMessageProcessed);
      assertThat(writer.getLogMessages(), is(messagesWritten));

      // Write another set of LogMessages.
      messagesWritten.addAll(getMessages(isKeyed ?
              writeThriftLogMessages(logger, processorBatchSize + 1, 500, 50) :
              writeThriftLogMessages(logger, processorBatchSize + 1, 50))
      );

      // Writer will throw on write.
      writer.setThrowOnWrite(true);
      LogPosition positionBefore = WatermarkUtils.loadCommittedPositionFromWatermark(
              DefaultLogStreamProcessor.getWatermarkFilename(logStream));
      try {
        processor.processLogStream();
        fail("No exception is thrown on writer error");
      } catch (LogStreamProcessorException e) {
        // Exception is thrown.
      }
      LogPosition positionAfter = WatermarkUtils.loadCommittedPositionFromWatermark(
              DefaultLogStreamProcessor.getWatermarkFilename(logStream));
      assertEquals(positionBefore, positionAfter);

      // Write will not throw on write.
      writer.setThrowOnWrite(false);
      numOfMessageProcessed = processor.processLogStream();
      assertEquals("Should not have processed any additional messages",
              processorBatchSize + 1, numOfMessageProcessed);
      assertThat(writer.getLogMessages(), is(messagesWritten));

      // Rotate and write twice before processing
      rotateWithDelay(logger, 1000);
      boolean successfullyAdded = messagesWritten.addAll(getMessages(isKeyed ?
              writeThriftLogMessages(logger, processorBatchSize - 20, 500, 50) :
              writeThriftLogMessages(logger, processorBatchSize - 20, 50))
      );
      assertTrue(successfullyAdded);
      rotateWithDelay(logger, 1000);
      successfullyAdded = messagesWritten.addAll(getMessages(isKeyed ?
              writeThriftLogMessages(logger, processorBatchSize, 500, 50) :
              writeThriftLogMessages(logger, processorBatchSize, 50))
      );
      assertTrue(successfullyAdded);

      // Need to wait for some time to make sure that messages have been written to disk
      Thread.sleep(FILE_EVENT_WAIT_TIME_MS);
      numOfMessageProcessed = processor.processLogStream();

      Thread.sleep(FILE_EVENT_WAIT_TIME_MS);
      processor.processLogStream();

      assertEquals(2 * processorBatchSize - 20, numOfMessageProcessed);
      assertThat(writer.getLogMessages(), is(messagesWritten));
      processor.processLogStream();
      String oldThriftLogPath = FilenameUtils.concat(getTempPath(), "thrift.log." + oldestThriftLogIndex);
      File oldThriftLog = new File(oldThriftLogPath);
      assertFalse(oldThriftLog.exists()); // the oldest file is at least 10 seconds old now
      //oldThriftLogPath = FilenameUtils.concat(getTempPath(),
      //        "thrift.log." + (oldestThriftLogIndex - 1));
      //oldThriftLog = new File(oldThriftLogPath);
      assertFalse(oldThriftLog.exists()); // the next oldest file is at least 9 seconds old now
      assertTrue(new File(path).exists()); // make sure the newest log file is still there
    } catch (Exception e) {
      e.printStackTrace();
      fail("Got exception in test");
    } finally {
      logger.close();
      processor.close();
    }
  }

  @Test
  public void testProcessLogStreamWithDecider() throws Exception {
    try {
      SingerConfig singerConfig = initializeSingerConfig(1, 1, Collections.emptyList());
      SingerSettings.initialize(singerConfig);
      SingerLog singerLog = new SingerLog(
          new SingerLogConfig("test", getTempPath(), "thrift.log", null, null, null));
      LogStream logStream = new LogStream(singerLog, "thrift.log");
      writer = new NoOpLogStreamWriter();
      initializeReaderAndProcessor(Collections.singletonMap("logDecider", "singer_test_decider"), logStream);
      Decider.setInstance(ImmutableMap.of("singer_test_decider", 0));
      // Write messages to be skipped.
      boolean deciderEnabled = processor.isLoggingAllowedByDecider();
      assertEquals(false, deciderEnabled);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected exception");
    } finally {
      if (processor != null) {
        processor.close();
      }
    }
  }

  @Test
  public void testDisableDecider() throws Exception {
    SingerUtils.setHostname("localhost-prod.cluster-19970722", "[.-]");
    try {
      SingerConfig singerConfig = initializeSingerConfig(1, 1, Collections.emptyList());
      SingerSettings.initialize(singerConfig);
      SingerLog singerLog = new SingerLog(
          new SingerLogConfig("test", getTempPath(), "thrift.log", null, null, null));
      LogStream logStream = new LogStream(singerLog, "thrift.log");
      writer = new NoOpLogStreamWriter();
      initializeReaderAndProcessor(Collections.singletonMap("logDecider", "singer_test_decider"), logStream);
      Decider.setInstance(new HashMap<>());
      Decider.getInstance().getDeciderMap().put("singer_test_decider", 100);
      assertEquals(true, processor.isLoggingAllowedByDecider());

      Decider.getInstance().getDeciderMap().put("singer_disable_test___localhost___decider", 100);
      assertEquals(false, processor.isLoggingAllowedByDecider());

      Decider.getInstance().getDeciderMap().put("singer_disable_test___localhost___decider", 50);
      Decider.getInstance().getDeciderMap().put("singer_disable_test___localhost_prod_cluster___decider", 100);
      assertEquals(false, processor.isLoggingAllowedByDecider());

    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected exception");
    } finally {
      if (processor != null) {
        processor.close();
      }
    }
    SingerUtils.setHostname(SingerUtils.getHostname(), "-");
  }

  @Test
  public void testProcessSymlinkLogStream() throws Exception {
    String tempPath = getTempPath();
    String symlinkLogStreamHeadFile = "thrift-symlink.log";
    String thriftLog = "thrift.log";
    String logPath = FilenameUtils.concat(tempPath, thriftLog);
    String symlinkPath = FilenameUtils.concat(tempPath, symlinkLogStreamHeadFile);

    int processorBatchSize = 50;

    // initialize a singer log config
    SingerLogConfig logConfig = new SingerLogConfig("test", tempPath, symlinkLogStreamHeadFile, null, null, null);
    SingerLog singerLog = new SingerLog(logConfig);
    singerLog.getSingerLogConfig().setFilenameMatchMode(FileNameMatchMode.PREFIX);

    // initialize global variables in SingerSettings
    try {
      SingerConfig singerConfig = initializeSingerConfig(1, 1, Arrays.asList(logConfig));
      SingerSettings.initialize(singerConfig);
    } catch (Exception e) {
      e.printStackTrace();
      fail("got exception in test: " + e);
    }

    SimpleThriftLogger<LogMessage> logger = new SimpleThriftLogger<>(logPath);
    writer = new NoOpLogStreamWriter();

    // Write some messages and wait
    for (int i = 0; i < 2; ++i) {
      writeThriftLogMessages(logger, processorBatchSize * 2, 50);
      Thread.sleep(FILE_EVENT_WAIT_TIME_MS);
    }

    Files.createSymbolicLink(new File(symlinkPath).toPath(), new File(logPath).toPath());

    // initialize log stream
    LogStream logStream = new LogStream(singerLog, symlinkLogStreamHeadFile);
    LogStreamManager.addLogStream(logStream);

    initializeReaderAndProcessor(Collections.singletonMap("processorBatchSize", String.valueOf(processorBatchSize)), logStream);

    waitForFileSystemEvents(logStream);

    // Process everything in the stream so far
    long numOfMessageProcessed = processor.processLogStream();
    assertEquals(processorBatchSize * 4, numOfMessageProcessed);


    // Delete the underlying thrift log, even if we wait after this the log stream
    // paths will not be updated by the FSM since the underlying log file is not tracked in the log stream paths
    Files.delete(new File(logPath).toPath());
    numOfMessageProcessed = processor.processLogStream();
    assertEquals(0, numOfMessageProcessed);

    // Recreate logger and write some messages
    logger = new SimpleThriftLogger<>(logPath);
    writeThriftLogMessages(logger, processorBatchSize, 50);
    Thread.sleep(FILE_EVENT_WAIT_TIME_MS);

    // Watermark positions should be equal since symlink would need to be recreated
    // to re-initialize the logstream
    LogPosition positionBefore = WatermarkUtils.loadCommittedPositionFromWatermark(
        DefaultLogStreamProcessor.getWatermarkFilename(logStream));
    processor.processLogStream();
    LogPosition positionAfter = WatermarkUtils.loadCommittedPositionFromWatermark(
        DefaultLogStreamProcessor.getWatermarkFilename(logStream));
    assertEquals(positionBefore, positionAfter);
  }

  private static List<LogMessage> getMessages(List<LogMessageAndPosition> messageAndPositions) {
    List<LogMessage> messages = Lists.newArrayListWithExpectedSize(messageAndPositions.size());
    for (LogMessageAndPosition messageAndPosition : messageAndPositions) {
      messages.add(messageAndPosition.getLogMessage());
    }
    return messages;
  }

  /*
   * Added to enable running this test on OS X
   */
  private static void waitForFileSystemEvents(LogStream logStream) throws InterruptedException {
    while (logStream.isEmpty()) {
      Thread.sleep(1000);
      System.out.print(".");
    }
  }
}