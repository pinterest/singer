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
package com.pinterest.singer.reader.mapped;

import java.io.IOException;
import java.nio.file.FileSystems;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.reader.LogFileReader;
import com.pinterest.singer.reader.LogFileReaderException;
import com.pinterest.singer.thrift.LogFile;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.LogMessageAndPosition;
import com.pinterest.singer.thrift.LogPosition;
import com.pinterest.singer.utils.SingerUtils;
import com.twitter.ostrich.stats.Stats;

/**
 * Reader that reads from thrift LogFile.
 * 
 * USE THIS READER FOR BATCH LOADING DATA ONLY, NOT FOR REAL-TIME STREAMING.
 * <p>
 * This class is NOT thread-safe.
 */
public class MappedThriftLogFileReader implements LogFileReader {

  private static final Logger LOG = LoggerFactory.getLogger(MappedThriftLogFileReader.class);

  // Factory that create LogMessage thrift objects.
  private static final class LogMessageFactory
      implements MappedThriftReader.TBaseFactory<LogMessage> {

    public LogMessage get() {
      return new LogMessage();
    }
  }

  private final LogFile logFile;
  private final String path;
  private final MappedThriftReader<LogMessage> thriftReader;

  /*
   * The maximum message size that is defined in singer configuration file
   */
  private final int maxMessageSize;

  /*
   * To tolerate messsages that exceed the size limit and minimize data loss,
   * Singer internally can read messages that 10 times larger than the specified
   * size limit. For the messages that exceeds the size limit, Singer drops these
   * messages and logs warnings, and do not send these messages to kafka.
   */
  private final int maxMessageSizeInternal;

  protected boolean closed;

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public MappedThriftLogFileReader(LogFile logFile,
                                   String path,
                                   long byteOffset,
                                   int readBufferSize,
                                   int maxMessageSize) throws Exception {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
    Preconditions.checkArgument(byteOffset >= 0);

    this.logFile = Preconditions.checkNotNull(logFile);
    this.path = path;
    this.maxMessageSize = maxMessageSize;
    this.maxMessageSizeInternal = maxMessageSize * 10;

    this.thriftReader = new MappedThriftReader(path, new LogMessageFactory(),
        maxMessageSizeInternal);
    this.thriftReader.setByteOffset((int) byteOffset);

    // Make sure the path is still associated with the LogFile.
    // This can happen when the path is reused for another LogFile during log
    // rotation.
    if (logFile.getInode() != SingerUtils.getFileInode(FileSystems.getDefault().getPath(path))) {
      LOG.info("Log file {} does not match path: {}. The path has been reused for another file.",
          logFile.getInode(), path);

      // Close the reader and throw.
      thriftReader.close();
      throw new LogFileReaderException(
          "Path: " + path + " is not associated with log file:" + logFile.toString());
    }
    closed = false;
  }

  @Override
  public LogMessageAndPosition readLogMessageAndPosition() throws LogFileReaderException {
    if (closed) {
      throw new LogFileReaderException("Reader closed.");
    }

    try {
      LogMessage logMessage = thriftReader.read();
      while (logMessage != null) {
        // Get the next LogMessage's byte offset
        long newByteOffset = thriftReader.getByteOffset();
        int messageSize = logMessage.getMessage().length;
        if (messageSize > maxMessageSize) {
          LOG.warn("Found a message at offset " + newByteOffset + "that exceeds the size limit in "
              + logFile.toString() + ": messageSize =  " + messageSize);
          OpenTsdbMetricConverter.incr("singer.thrift_reader.skip_message", 1, "path=" + path);
          logMessage = thriftReader.read();
        } else {
          LogPosition position = new LogPosition(logFile, newByteOffset);
          return new LogMessageAndPosition(logMessage, position);
        }
      }
    } catch (TException e) {
      LOG.error("Caught TException while reading " + logFile, e);
      OpenTsdbMetricConverter.incr("singer.reader.exception.texception", 1, "path=" + path);
      throw new LogFileReaderException("Cannot read a log message.", e);
    } catch (Exception e) {
      LOG.error("Caught exception when read a log message from log file: " + logFile, e);
      Stats.incr("singer.reader.exception.unexpected");
      throw new LogFileReaderException("Cannot read a log message.", e);
    }
    return null;
  }

  public void close() throws IOException {
    if (closed) {
      return;
    }

    thriftReader.close();
    closed = true;
  }

  @Override
  public LogFile getLogFile() throws LogFileReaderException {
    if (closed) {
      throw new LogFileReaderException("Reader closed.");
    }

    return logFile;
  }

  @Override
  public long getByteOffset() throws LogFileReaderException {
    if (closed) {
      throw new LogFileReaderException("Reader closed.");
    }
    try {
      return thriftReader.getByteOffset();
    } catch (Exception e) {
      LOG.error("Caught exception when get reader byte offset of log file: " + logFile, e);
      Stats.incr("singer.reader.exception.unexpected");
      throw new LogFileReaderException("Can not get byte offset of the thrift reader", e);
    }
  }

  @Override
  public void setByteOffset(long byteOffset) throws LogFileReaderException {
    if (closed) {
      throw new LogFileReaderException("Reader closed.");
    }
    // TODO check overflow
    try {
      thriftReader.setByteOffset((int) byteOffset);
    } catch (Exception e) {
      LOG.error(String.format("Caught exception when set reader byte offset of log file: %s to: %d",
          logFile, byteOffset), e);
      Stats.incr("singer.reader.exception.unexpected");
      throw new LogFileReaderException("Can not set byte offset on the thrift reader", e);
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }
}