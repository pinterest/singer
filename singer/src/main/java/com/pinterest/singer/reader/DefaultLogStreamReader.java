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

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.LogStreamReader;
import com.pinterest.singer.common.errors.LogStreamReaderException;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.LogFile;
import com.pinterest.singer.thrift.LogFileAndPath;
import com.pinterest.singer.thrift.LogMessageAndPosition;
import com.pinterest.singer.thrift.LogPosition;
import com.pinterest.singer.utils.LogFileUtils;
import com.pinterest.singer.utils.SingerUtils;

import com.google.common.base.Preconditions;
import com.twitter.ostrich.stats.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * The default implementation of DefaultLogStreamReader that read LogFiles in LogStream one by
 * one in the order of LogFile sequence.
 * <p/>
 * This class is NOT thread-safe.
 */
public class DefaultLogStreamReader implements LogStreamReader {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultLogStreamReader.class);

  // LogStream read by this DefaultLogStreamReader.
  private final LogStream logStream;

  // Factory object to get LogFileReader implementations.
  private final LogFileReaderFactory fileReaderFactory;

  // Whether this DefaultLogStreamReader is closed.
  private boolean closed;

  // Reader for the current LogFile. This can be null when the DefaultLogStreamReader is
  // closed or the LogStream does not have any LogFile.
  private LogFileReader fileReader;

  public DefaultLogStreamReader(LogStream logStream, LogFileReaderFactory fileReaderFactory) {
    this.fileReaderFactory = Preconditions.checkNotNull(fileReaderFactory);
    this.logStream = Preconditions.checkNotNull(logStream);
    this.fileReader = null;
    this.closed = false;
  }

  @Override
  public LogStream getLogStream() {
    return logStream;
  }

  @Override
  public LogPosition getPos() throws LogStreamReaderException {
    if (closed) {
      throw new LogStreamReaderException("Reader closed.");
    }
    if (fileReader == null) {
      return null;
    }

    try {
      return new LogPosition(fileReader.getLogFile(), fileReader.getByteOffset());
    } catch (Exception e) {
      LOG.error("Caught exception when get current read position of log stream: " + logStream, e);
      try {
        fileReader.close();
        fileReader = null;
        LOG.info("Closed fileReader when failed to getPos on DefaultLogStreamReader");
      } catch (IOException ex) {
        throw new LogStreamReaderException("Can not close fileReader.", ex);
      }
      throw new LogStreamReaderException(
          "Cannot get current log position for " + logStream.getLogStreamDescriptor(), e);
    }
  }

  @Override
  public void seek(LogPosition position) throws LogStreamReaderException {
    if (closed) {
      throw new LogStreamReaderException("Reader closed.");
    }

    LogFile logFile = position.getLogFile();
    if (!logStream.hasLogFile(logFile)) {
      // if the logFile is not in logstream, it is possible that the logstream is in
      // the middle of log file rotation, and the log stream head file was just got deleted.
      throw new LogStreamReaderException("Invalid position: " + position
          + " in Log stream: " + logStream.getLogStreamDescriptor());
    }

    try {
      // Get the LogFile path from LogStream.
      String logFilePath = logStream.getLogFilePath(logFile);
      long offset = position.getByteOffset();

      // Set LogFile reader's read position.
      if (fileReader == null || fileReader.isClosed()) {
        fileReader = fileReaderFactory.getLogFileReader(logStream, logFile, logFilePath, offset);
      } else if (fileReader.getLogFile().equals(position.getLogFile())) {
        fileReader.setByteOffset(offset);
      } else {
        fileReader.close();
        fileReader = fileReaderFactory.getLogFileReader(logStream, logFile, logFilePath, offset);
      }
    } catch (Exception e) {
      LOG.error("Caught exception when set current read position of log stream: " + logStream, e);
      try {
        if (fileReader != null) {
          fileReader.close();
          fileReader = null;
          LOG.info("Closed fileReader when failed to seek on DefaultLogStreamReader");
        }
      } catch (IOException ex) {
        throw new LogStreamReaderException("Can not close fileReader.", ex);
      }
      throw new LogStreamReaderException("Can not seek to position: " + position.toString(), e);
    }
  }

  private LogFileReader getNextLogFileReader(LogFileAndPath nextLogFileAndPath) throws Exception {
    LogFileReader reader = null;
    if (nextLogFileAndPath != null) {
      LogFile logFile = nextLogFileAndPath.getLogFile();
      String path = nextLogFileAndPath.getPath();
      try {
        reader = fileReaderFactory.getLogFileReader(logStream, logFile, path, 0L);
      } catch (LogFileReaderException e) {
        LOG.warn("Exception in getNextLogFileReader", e);
        long inode = nextLogFileAndPath.getLogFile().getInode();
        String newPath = logStream.getLogFilePath(nextLogFileAndPath.getLogFile());
        long actualInode = SingerUtils.getFileInode(newPath);
        if (actualInode != inode) {
          newPath = LogFileUtils.getFilePathByInode(logStream, inode);
        }
        if (newPath == null) {
          LOG.error("There is no file for inode " + inode, e);
        } else {
          LOG.info("Found file " + newPath + "for inode " + inode);
          nextLogFileAndPath.setPath(newPath);
          reader = getNextLogFileReader(nextLogFileAndPath);
        }
      }
    }
    return reader;
  }


  @Override
  public LogMessageAndPosition readLogMessageAndPosition() throws LogStreamReaderException {
    if (closed) {
      throw new LogStreamReaderException("Reader closed.");
    }
    if (fileReader == null) {
      return null;
    }

    try {
      // Read a LogMessage from the current LogFile using the file reader.
      LogMessageAndPosition messageAndPosition = null;
      LogFile currentLogFile = fileReader.getLogFile();
      long startByteOffset = fileReader.getByteOffset();

      try {
        messageAndPosition = fileReader.readLogMessageAndPosition();
        long bytesRead = fileReader.getByteOffset() - startByteOffset;
        LOG.trace("Read {} bytes from {}", bytesRead, fileReader.getLogFile());
      } catch (Exception e) {
        LOG.error("Caught exception while reading from log file: {}", currentLogFile, e);
      }

      if (messageAndPosition == null) {
        // Close the fileReader when we can not read a message from the LogFile.
        // This will close the fileReader when we reach the end of a LogFile.
        fileReader.close();
        fileReader = null;

        LogFileAndPath nextLogFileAndPath = logStream.getNext(currentLogFile);
        if (nextLogFileAndPath == null) {
          // If there is no LogFile after this one, we have no LogMessage to read for now. We
          // keep the file reader open in the hope that someone will append to the current log
          // file which is the last log file in the stream.
          LOG.info("No more message in the current log file and it is the end of log stream: {}",
              logStream);
          return null;
        }

        fileReader = getNextLogFileReader(nextLogFileAndPath);
        if (fileReader == null) {
          Stats.incr("singer.processor.files.missing");
          return null;
        }

        LOG.info("Continue to read from next log file {} in log stream {}", fileReader.getLogFile(),
            logStream.getLogStreamDescriptor());

        // Number of bytes skipped in the current file.
        long currentFileLength = new File(logStream.getLogFilePath(currentLogFile)).length();
        long bytesSkipped = currentFileLength - startByteOffset;
        if (bytesSkipped > 0) {
          LOG.error("Skipped {} bytes in log file: {} in log stream: {}", bytesSkipped, currentLogFile, logStream);
          OpenTsdbMetricConverter.incr(SingerMetrics.SKIPPED_BYTES, (int) bytesSkipped,
              "log=" + logStream.getSingerLog().getSingerLogConfig().getName(),
              "host=" + SingerUtils.getHostname());
        }
        // Read from next LogFile.
        return readLogMessageAndPosition();
      } else {
        return messageAndPosition;
      }
    } catch (Exception e) {
      LOG.error("Caught exception while reading log message from log stream: {}", logStream, e);
      try {
        if (fileReader != null) {
          fileReader.close();
        }
        fileReader = null;
        LOG.info("Closed fileReader when failed to seek on DefaultLogStreamReader");
      } catch (IOException ex) {
        throw new LogStreamReaderException("Can not close fileReader.", ex);
      }
      throw new LogStreamReaderException(
          "Can not read a log message from current position in log stream: " + logStream
              .getLogStreamDescriptor(), e);
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    if (fileReader != null) {
      fileReader.close();
      fileReader = null;
    }
    closed = true;
  }
}
