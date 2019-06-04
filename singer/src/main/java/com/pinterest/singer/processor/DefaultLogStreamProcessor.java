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

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.LogStreamProcessor;
import com.pinterest.singer.common.errors.LogStreamProcessorException;
import com.pinterest.singer.common.LogStreamReader;
import com.pinterest.singer.common.errors.LogStreamReaderException;
import com.pinterest.singer.common.LogStreamWriter;
import com.pinterest.singer.common.errors.LogStreamWriterException;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.config.Decider;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.LogFileAndPath;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.LogMessageAndPosition;
import com.pinterest.singer.thrift.LogPosition;
import com.pinterest.singer.utils.WatermarkUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.twitter.ostrich.stats.Stats;
import org.apache.commons.io.FilenameUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation of LogStreamProcessor which periodically wakes up and processes the
 * LogStream till the current end of it.
 * <p/>
 * This class is not thread-safe. processLogStream() method does all the processing job and
 * should only be called from one thread at any time. The start() and stop() methods can be called
 * in other threads to start and stop the processor .
 */
public class DefaultLogStreamProcessor implements LogStreamProcessor, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultLogStreamProcessor.class);

  // Decider for the log stream.
  private final String logDecider;

  // LogStream to be processed.
  private final LogStream logStream;

  // Reader for the LogStream.
  private final LogStreamReader reader;

  // Writer for the LogStream.
  private final LogStreamWriter writer;

  // Processor batch size.
  private int batchSize;
  private final int batchSizeOriginal;

  // Randomizer for initial processing delay.
  private final Random random;

  // Processor process interval in milliseconds.
  private long processingIntervalInMillis;

  // Processor process interval in milliseconds.
  private final long processingIntervalInMillisMin;

  // Processor process interval in milliseconds.
  private final long processingIntervalInMillisMax;

  // Processor process time slice in milliseconds.
  private final long processingTimeSliceInMilliseconds;

  // a boolean flag on whether LogStremaProcess uses up the time slice or not
  private boolean exceedTimeSliceLimit;

  // the log retention time in seconds
  private final int logRetentionInSecs;

  // Executor which executes processing tasks.
  private final ScheduledExecutorService executorService;

  // Whether this processor is stopped.
  private Boolean isStopped;

  // Whether we are in a processing cycle. This is made volatile for thread visibility.
  // We read this volatile variable at the beginning of a processing cycle and write to it at
  // the end of a process cycle. This will make all writes from last processing cycle visible to
  // the next processing cycle when this volatile variable is read in the next cycle.
  private volatile boolean cycleStarted;

  // Handle to the next scheduled processor run.
  private ScheduledFuture<?> scheduledFuture;

  // Committed LogPosition so far in the LogStream.
  private LogPosition committedPosition;

  // Counter of LogMessages that have been committed since this processor starts.
  private long numOfLogMessagesCommitted;

  // The last modification time of the stream which we have already successfully processed up to. -1
  // if no processing happened.
  private final AtomicLong lastModificationTimeProcessed;

  // The sys time we finished last cycle work.
  private final AtomicLong lastCompletedCycleTime;

  public DefaultLogStreamProcessor(
      LogStream logStream,
      String logDecider,
      LogStreamReader reader,
      LogStreamWriter writer,
      int batchSize,
      long processingIntervalInMillisMin,
      long processingIntervalInMillisMax,
      long processingTimeSliceInMilliseconds,
      int logRetentionInSecs) {
    Preconditions.checkArgument(batchSize > 0);
    Preconditions.checkArgument(processingIntervalInMillisMin > 0);
    Preconditions.checkArgument(processingIntervalInMillisMax >= processingIntervalInMillisMin);
    this.logStream = Preconditions.checkNotNull(logStream);
    this.reader = Preconditions.checkNotNull(reader);
    this.writer = Preconditions.checkNotNull(writer);
    this.logDecider = logDecider;
    this.batchSize = batchSize;
    this.batchSizeOriginal = batchSize;
    this.random = new Random();
    this.processingIntervalInMillisMin = processingIntervalInMillisMin;
    this.processingIntervalInMillisMax = processingIntervalInMillisMax;
    this.processingIntervalInMillis = processingIntervalInMillisMin;
    this.processingTimeSliceInMilliseconds = processingTimeSliceInMilliseconds;
    this.logRetentionInSecs = logRetentionInSecs;
    this.executorService = Preconditions.checkNotNull(SingerSettings.getLogProcessorExecutor());
    this.isStopped = true;
    this.scheduledFuture = null;
    this.committedPosition = null;
    this.numOfLogMessagesCommitted = 0;
    this.isStopped = true;
    this.cycleStarted = false;
    this.exceedTimeSliceLimit = false;
    this.lastModificationTimeProcessed = new AtomicLong(-1);
    this.lastCompletedCycleTime = new AtomicLong(-1);
  }

  @Override
  public LogStream getLogStream() {
    return logStream;
  }

  /**
   * Process log stream till no new LogMessage in the log stream or we run into error during
   * processing.
   * <p/>
   * This method should be called from one thread at any time.
   *
   * @return Number of LogMessages processed.
   * @throws LogStreamProcessorException when run into processing error.
   */
  @Override
  public long processLogStream() throws LogStreamProcessorException {
    try {
      LOG.info("Start a processing cycle for log stream: {}", logStream);
      long cycleStartTime = System.currentTimeMillis();
      exceedTimeSliceLimit = false;

      // If LogStream has no LogFile, skip this processing cycle.
      if (logStream.isEmpty()) {
        LOG.info("Log stream: {} is empty. Skipping this processing cycle.", logStream);
        return 0;
      }

      // Load last committed position from watermark.
      committedPosition = loadCommittedPosition();
      LOG.info("Log stream: {}'s last committed position is: {}.", logStream, committedPosition);

      // Seek to committed position.
      reader.seek(committedPosition);
      LOG.info("Set log stream: {}'s read position to: {}.", logStream, committedPosition);

      LogPosition cycleStartPosition = committedPosition;
      long cycleStartNumOfLogMessagesCommitted = numOfLogMessagesCommitted;

      // Process the LogStream until there is no LogMessage remaining in the stream or we run into error.
      while (processLogMessageBatch() >= batchSize) {
        long currentTime = System.currentTimeMillis();
        if (currentTime > cycleStartTime + processingTimeSliceInMilliseconds) {
          LOG.info("Log stream {} used up {} milliseconds time slice.", logStream , processingTimeSliceInMilliseconds);
          exceedTimeSliceLimit = true;
          break;
        }
      }

      LOG.info("Done with current processing cycle for log stream: {}. Processed {} log messages "
          + "from position: {} to position: {}. lastModificationTimeProcessed is advanced to {}.",
          this.logStream,
          numOfLogMessagesCommitted - cycleStartNumOfLogMessagesCommitted,
          cycleStartPosition,
          committedPosition,
          logStream.getLastStreamModificationTime());

      // Advance the progress marker to the last modification time of the stream snapshot.
      lastModificationTimeProcessed.set(logStream.getLastStreamModificationTime());
      if (logRetentionInSecs > 0) {
        // Remove old log files in the current stream where mTime >= logRetentionInSecs
        logStream.removeOldFiles(committedPosition.logFile, logRetentionInSecs);
      }
      return numOfLogMessagesCommitted - cycleStartNumOfLogMessagesCommitted;
    } catch (LogStreamReaderException e) {
      LOG.error("Failed to seek to position " + committedPosition, e);
    } catch (Exception e) {
      LOG.error("Caught unexpected exception while processing " + logStream, e);
    }
    throw new LogStreamProcessorException(
        "Failed to process log stream: " + logStream.getLogStreamDescriptor());
  }

  /**
   * If the decider is not set, this method will return true.
   * If a decider is set, only return false when the decider's value is 0.
   *
   * @return true or false.
   */
  boolean isLoggingAllowedByDecider() {
    boolean result = true;
    if (logDecider != null && !logDecider.isEmpty()) {
      Map<String, Integer> map = Decider.getInstance().getDeciderMap();
      if (map.containsKey(logDecider)) {
        result = map.get(logDecider) != 0;
      }
    }
    return result;
  }

  @Override
  public void run() {
    long logMessagesProcessed = -1;
    try {
      if (!cycleStarted) {
        cycleStarted = true;
      }
      if (isLoggingAllowedByDecider()) {
        // process this log stream
        logMessagesProcessed = processLogStream();
        if (!exceedTimeSliceLimit) {
          lastCompletedCycleTime.set(System.currentTimeMillis());
        }
      } else {
        logMessagesProcessed = 0;
        LOG.info("Log stream {} is disabled by decider {}", logStream.getLogStreamDescriptor(),
            logDecider);
      }
    } catch (LogStreamProcessorException e) {
      LOG.error("LogStreamProcessorException in processing log stream: " + logStream, e);
      Stats.incr(SingerMetrics.PROCESSOR_EXCEPTION);
    } catch (Exception e) {
      LOG.error("Caught unexpected exception while processing " + logStream, e);
      Stats.incr("singer.processor.unexpected_exception");
    } catch (Throwable t) {
      LOG.error("Caught unexpected throwable while processing " + logStream, t);
      System.exit(1);
    } finally {
      cycleStarted = false;
      if (logMessagesProcessed == -1) {
        batchSize = Math.max(1, batchSize / 2);
        LOG.warn("Processing error, decrease batch size to " + batchSize);
      } else if (batchSize != batchSizeOriginal) {
        batchSize = batchSizeOriginal;
        LOG.warn("Restoring batch size to " + batchSize);
      }
      long newProcessingIntervalInMillis = processingIntervalInMillis;
      // Adjust processing interval
      if (logMessagesProcessed == 0L) {
        // If we processed 0 messages at this processing cycle, we use a simply algorithm to
        // double the processing interval for this log stream. This will introduce processing
        // latency at most the message incoming interval.
        newProcessingIntervalInMillis =
            Math.min(processingIntervalInMillisMax, processingIntervalInMillis * 2);
      } else if (logMessagesProcessed > 0L) {
        // If got at least one message, reset the processing interval to min value to restart.
        newProcessingIntervalInMillis = processingIntervalInMillisMin;
      }

      if (newProcessingIntervalInMillis != processingIntervalInMillis) {
        // We have a new processing interval.
        processingIntervalInMillis = newProcessingIntervalInMillis;
        LOG.info("Log stream: {} is processed at new interval: {} milliseconds",
            logStream.getLogStreamDescriptor(), processingIntervalInMillis);
        // We introduce a random initial delay to jitterize the processing cycle.
        long initialDelay = Math.abs(random.nextLong()) % processingIntervalInMillis;
        scheduledFuture = executorService.schedule(this, initialDelay, TimeUnit.MILLISECONDS);
        LOG.info("Log stream: {}'s next processing cycle is scheduled with initial delay: {}",
            logStream.getLogStreamDescriptor(), initialDelay);
      } else {
        // Schedule next run after processing interval.
        scheduledFuture =
            executorService.schedule(this, processingIntervalInMillis, TimeUnit.MILLISECONDS);
        LOG.info("Log stream: {}'s next processing cycle is scheduled after: {} milliseconds",
            logStream.getLogStreamDescriptor(), processingIntervalInMillis);
      }
    }
  }

  @Override
  public long getLastCompleteCycleTime() {
    return this.lastCompletedCycleTime.get();
  }

  /**
   * Start to periodically process the LogStream.
   * <p/>
   * This method is thread-safe.
   */
  @Override
  public void start() {
    synchronized (isStopped) {
      if (isStopped) {
        // Start the processor after a random initial delay between 0 and
        // processingIntervalInSeconds.
        long initialDelay = Math.abs(random.nextLong()) % this.processingIntervalInMillis;
        scheduledFuture = executorService.schedule(this, initialDelay, TimeUnit.MILLISECONDS);
        isStopped = false;
        LOG.info("Start log processor for log stream: {} which process logs every {} milliseconds "
                + "with initial delay: {} milliseconds.",
            logStream, processingIntervalInMillis, initialDelay);
      } else {
        LOG.warn("LogStreamProcessor already started when asked to start.");
      }
    }
  }

  /**
   * Stop process the LogStream.
   * <p/>
   * This method is thread safe.
   */
  @Override
  public void stop() {
    synchronized (isStopped) {
      if (!isStopped) {
        Preconditions.checkState(scheduledFuture != null,
            "LogStreamProcessor for LogStream is not scheduled after the LogStreamProcessor starts");
        // Do not interrupt if the log processor is running now.
        scheduledFuture.cancel(false);

        // Wait until the last scheduled run is done.
        try {
          scheduledFuture.get();
        } catch (InterruptedException e) {
          // Ignore any exception from last scheduled run.
          LOG.error("Interrupted from last processing cycle.", e);
        } catch (ExecutionException e) {
          LOG.error("Execution exception from last processing cycle.", e);
        } catch (CancellationException e) {
          // this is not unexpected
        } catch (Exception e) {
          LOG.error("Caught an unexpected exception", e);
          Stats.incr("singer.processor.unexpected_exception");
        }
        isStopped = true;
        LOG.info("Stopped log processor for log stream: {}", logStream);
      } else {
        LOG.warn("LogStreamProcessor already stopped when asked to stop.");
      }
    }
  }

  @Override
  public void close() throws IOException {
    reader.close();
    writer.close();
  }

  @Override
  public long getLastModificationTimeProcessed() {
    return lastModificationTimeProcessed.get();
  }

  /**
   * Get the globally unique watermark filename
   * @param logStream logStream
   * @return
   */
  static String getWatermarkFilename(LogStream logStream) {
    String path = logStream.getSingerLog().getSingerLogConfig().getLogDir();
    // Get the globally unique watermark file name
    String watermarkFilename = "." + logStream.getSingerLog().getLogName()
        + "." + logStream.getLogStreamName();
    return FilenameUtils.concat(path, watermarkFilename);
  }

  private LogPosition resetLogStreamWatermarkPosition(LogStream stream) {
    LOG.warn("Reset log stream watermark : {}", stream);
    LogFileAndPath firstLogFileAndPath = stream.getFirstLogFile();
    Preconditions.checkNotNull(firstLogFileAndPath);
    LogPosition position = new LogPosition(firstLogFileAndPath.getLogFile(), 0L);
    LOG.info("Process log stream: {} from beginning of the stream: {}", stream, position);
    OpenTsdbMetricConverter.incr(
        "singer.processor.committed_position_reset", 1, "log=" + logStream.getLogStreamName());
    return position;
  }

  /**
   * Load last committed position from watermark file. If we can not load committed position from
   * watermark file,
   * start from the beginning of the first LogFile in this log stream.
   *
   * @return LogPosition in watermark file.
   */
  private LogPosition loadCommittedPosition() {
    LogPosition position;
    try {
      String wmFilePath = getWatermarkFilename(logStream);
      position = WatermarkUtils.loadCommittedPositionFromWatermark(wmFilePath);

      // Check if the watermark points to a LogFile in the log stream.
      if (!logStream.hasLogFile(position.getLogFile())) {
        Thread.sleep(SingerSettings.getSingerConfig().logFileRotationTimeInMillis);
        logStream.initialize();
        if (!logStream.hasLogFile(position.getLogFile())) {
          LOG.warn("The position: {} from watermark is invalid in {}.", position, logStream);
          position = resetLogStreamWatermarkPosition(logStream);
        }
      }
    } catch (FileNotFoundException e) {
      LOG.warn("Failed to find watermark file for " + logStream, e.getMessage());
      Stats.incr("singer.processor.log_streams_discovered");
      position = resetLogStreamWatermarkPosition(logStream);
      try {
        // if the watermark file is missing, write position info to disk to
        commitLogPosition(position, true);
      } catch (Exception ex) {
        // This exception is not fatal. Singer will retry committing offsets later.
        // Because of this, we only log an error and continue.
        LOG.error("Failed to persis watermark {} for {}", position, logStream, ex);
      }
    } catch (Exception e) {
      LOG.error("Exception in loading committed position for " + logStream, e);
      position = resetLogStreamWatermarkPosition(logStream);
    }
    return position;
  }

  /**
   * Process a batch of LogMessages.
   *
   * @return number of LogMessage processed.
   * @throws Exception when failed to process the message batch.
   */
  private int processLogMessageBatch() throws IOException, LogStreamWriterException, TException {
    LOG.debug("Start processing a batch of log messages in log stream: {} starting at position: {}",
        logStream, committedPosition);
    LogPosition batchStartPosition = committedPosition;
    List<LogMessageAndPosition> logMessagesRead = Lists.newArrayListWithExpectedSize(batchSize);

    // Read a batch of LogMessages.
    readLogMessages(logMessagesRead);

    if (logMessagesRead.size() > 0) {
      // Write the batch of LogMessages.
      writeLogMessages(logMessagesRead);

      // The new committed position is the position after the last written LogMessage.
      LogPosition newCommittedPosition =
          logMessagesRead.get(logMessagesRead.size() - 1).getNextPosition();

      commitLogPosition(newCommittedPosition, true);
      numOfLogMessagesCommitted += logMessagesRead.size();
      LOG.debug("Done processing {} log messages in LogStream {} from position {} to position {}.",
          logMessagesRead.size(), this.logStream, batchStartPosition, committedPosition);
    } else {
      LOG.debug("Done processing log messages in LogStream {} : no new messages.", this.logStream);
    }
    return logMessagesRead.size();
  }

  /**
   * Read a batch of LogMessages from LogStream starting from the current reading position.
   *
   * @param logMessagesRead LogMessages read from LogStream
   * @return whether successfully read the batch.
   */
  private boolean readLogMessages(List<LogMessageAndPosition> logMessagesRead) {
    boolean retval = false;
    try {
      for (int i = 0; i < this.batchSize; ++i) {
        LogMessageAndPosition message = reader.readLogMessageAndPosition();
        if (message == null) {
          // We run out of LogMessage, we are done with this processing cycle.
          break;
        } else {
          logMessagesRead.add(message);
        }
      }
      retval = true;
    } catch (Exception e) {
      String errorString =
          "Caught exception when reading the current batch of messages from " + logStream;
      if (logMessagesRead.size() > 0) {
        errorString += "The last good log position is: "
            + logMessagesRead.get(logMessagesRead.size() - 1).getNextPosition()
            + ". Abort this processing cycle after sending the log messages we get so far.";
      } else {
        errorString += "Abort this processing cycle without reading any messages.";
      }
      LOG.error(errorString, e);
    }
    return retval;
  }

  /**
   * Write a batch of LogMessages.
   *
   * @param logMessagesRead LogMessages to be written.
   * @throws LogStreamWriterException when fail to write the LogMessages.
   */
  private void writeLogMessages(List<LogMessageAndPosition> logMessagesRead)
      throws LogStreamWriterException {
    int numMessages = logMessagesRead.size();
    if (numMessages <= 0) {
      return;
    }
    List<LogMessage> logMessagesToWrite = Lists.newArrayListWithExpectedSize(numMessages);
    for (LogMessageAndPosition logMessageRead : logMessagesRead) {
      logMessagesToWrite.add(logMessageRead.getLogMessage());
    }
    writer.writeLogMessages(logMessagesToWrite);
    LogMessage lastMessage = logMessagesToWrite.get(numMessages - 1);
    if (lastMessage.isSetTimestampInNanos()) {
      logStream.setLatestProcessedMessageTime(lastMessage.getTimestampInNanos() / 1000000);
    }
  }

  /**
   * Commit the specified LogPosition.
   *
   * @param position   LogPosition to be committed.
   * @param persistent Whether the position should be saved to watermark file.
   * @throws Exception when fail to commit the LogPosition.
   */
  private void commitLogPosition(LogPosition position, boolean persistent)
      throws IOException, TException {
    this.committedPosition = position;
    if (persistent) {
      WatermarkUtils.saveCommittedPositionToWatermark(
          getWatermarkFilename(this.logStream), this.committedPosition);
    }
  }
}
