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
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.LogFile;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.LogMessageAndPosition;
import com.pinterest.singer.thrift.LogPosition;
import com.pinterest.singer.thrift.TextMessage;
import com.pinterest.singer.thrift.configuration.TextLogMessageType;
import com.pinterest.singer.utils.SingerUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Reader that read from line oriented text file using UTF8 encoding.
 */
public class TextLogFileReader implements LogFileReader {

  private static final int MAX_BUFFER_HEADROOM = 300;

  private static final Logger LOG = LoggerFactory.getLogger(TextLogFileReader.class);

  protected boolean closed;
  private final LogFile logFile;
  private final LogStream logStream;
  private final String path;
  private final int numMessagesPerLogMessage;
  private final boolean prependTimestamp;
  private final boolean prependHostname;
  private final String prependFieldDelimiter;
  private final TSerializer serializer;
  private final TextMessageReader textMessageReader;
  private final Pattern filterMessageRegex;
  private ByteBuffer maxBuffer;

  // The text log message format, can be TextMessage, or String;
  private final TextLogMessageType textLogMessageType;

  private String hostname;
  private String availabilityZone;
  private boolean trimTailingNewlineCharacter;

  private Map<String, ByteBuffer> headers;

  public TextLogFileReader(
                           LogStream logStream,
                           LogFile logFile,
                           String path,
                           long byteOffset,
                           int readBufferSize,
                           int maxMessageSize,
                           int numMessagesPerLogMessage,
                           Pattern messageStartPattern,
                           Pattern filterMessageRegex,
                           TextLogMessageType messageType,
                           boolean prependTimestamp,
                           boolean prependHostName,
                           boolean trimTailingNewlineCharacter,
                           String hostname,
                           String availabilityZone,
                           String prependFieldDelimiter,
                           Map<String, ByteBuffer> headers) throws Exception {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
    Preconditions.checkArgument(byteOffset >= 0);

    this.logStream = logStream;
    this.headers = headers;
    if (headers != null) {
      headers.put("hostname", SingerUtils.getByteBuf(hostname));
      headers.put("file", SingerUtils.getByteBuf(path));
      headers.put("availabilityZone", SingerUtils.getByteBuf(availabilityZone));
    }

    this.hostname = hostname;
    this.availabilityZone = availabilityZone;
    this.logFile = Preconditions.checkNotNull(logFile);
    this.path = path;
    this.numMessagesPerLogMessage = numMessagesPerLogMessage;
    this.serializer = new TSerializer();
    this.textMessageReader = new TextMessageReader(logStream, path, readBufferSize, maxMessageSize,
        messageStartPattern);
    this.textMessageReader.setByteOffset(byteOffset);
    this.textLogMessageType = messageType;
    this.prependTimestamp = prependTimestamp;
    this.prependHostname = prependHostName;
    this.prependFieldDelimiter = prependFieldDelimiter;
    int capacity = (maxMessageSize * numMessagesPerLogMessage) + MAX_BUFFER_HEADROOM;
    this.maxBuffer = ByteBuffer.allocate(capacity);
    this.trimTailingNewlineCharacter = trimTailingNewlineCharacter;
    this.filterMessageRegex = filterMessageRegex;

    // Make sure the path is still associated with the LogFile.
    // This can happen when the path is reused for another LogFile during log
    // rotation.
    if (logFile.getInode() != SingerUtils.getFileInode(SingerUtils.getPath(path))) {
      LOG.info(
          "Log file {} does not match path: {}. "
              + "This can happen when the path is reused for another log file.",
          logFile.getInode(), path);

      // Close the textMessageReader and throw.
      textMessageReader.close();
      throw new LogFileReaderException(
          "Path: " + path + " is not associated with log file: " + logFile.toString());
    }
    closed = false;
  }

  @Override
  public LogMessageAndPosition readLogMessageAndPosition() throws LogFileReaderException {
    if (closed) {
      throw new LogFileReaderException("Reader for " + path + " is closed.");
    }

    try {
      TextMessageReader.resetByteBuffer(maxBuffer);
      boolean skipLogMessage = false;
      for (int i = 0; i < numMessagesPerLogMessage; ++i) {
        ByteBuffer message = textMessageReader.readMessage(true);
        // If no message in the file, break.
        if (message == null) {
          break;
        }
        // If the message does not match the filter regex, mark it to be skipped.
        if (filterMessageRegex != null && !filterMessageRegex.matcher(
            TextMessageReader.bufToString(message)).matches()) {
          skipLogMessage = true;
        }
        String prependStr = "";
        if (prependTimestamp) {
          prependStr += System.currentTimeMillis() + prependFieldDelimiter;
        }
        if (prependHostname) {
          prependStr += hostname + prependFieldDelimiter;
        }
        if (prependStr.length() > 0) {
          maxBuffer.put(prependStr.getBytes());
        }
        maxBuffer.put(message);
      }

      if (maxBuffer.position() <= 0) {
        // No messages in the file, return null.
        return null;
      }

      maxBuffer.flip();
      // we have to copy the buffer here since LogMessages are batched therefore,
      // the returned buffer can't be reused since it would lead to data corruption
      ByteBuffer out = ByteBuffer.allocate(maxBuffer.limit()).put(maxBuffer);
      out.flip();
      if (trimTailingNewlineCharacter && out.get(out.limit() - 1) == '\n') {
        out.limit(out.limit() - 1);
      }

      LogMessage logMessage = null;
      switch (textLogMessageType) {
      case THRIFT_TEXT_MESSAGE:
        TextMessage textMessage = new TextMessage();
        textMessage.setFilename(path);
        textMessage.setHost(hostname);
        textMessage.setAvailabilityZone(availabilityZone);
        textMessage.addToMessages(TextMessageReader.bufToString(out));
        logMessage = new LogMessage(ByteBuffer.wrap(serializer.serialize(textMessage)));
        break;
      case PLAIN_TEXT:
        logMessage = new LogMessage(out);
        break;
      default:
        throw new UnsupportedOperationException(
            "Unknown text log message type:" + textLogMessageType.name());
      }
      // Get the next message's byte offset
      LogPosition position = new LogPosition(logFile, textMessageReader.getByteOffset());
      LogMessageAndPosition logMessageAndPosition = new LogMessageAndPosition(logMessage, position);
      // Inject an immutable map with a single skipMessage header so that processors can skip this message
      // we initialize it here in case environmentVariableInjection is disabled
      if (skipLogMessage) {
        logMessageAndPosition.setInjectedHeaders(
            ImmutableMap.of("skipMessage", ByteBuffer.wrap(new byte[0])));
        OpenTsdbMetricConverter.incr("singer.reader.text.filtered_messages",
            "logName=" + logStream.getSingerLog().getSingerLogConfig().getName());
      } else {
        logMessageAndPosition.setInjectedHeaders(headers);
      }
      return logMessageAndPosition;
    } catch (Exception e) {
      LOG.error("Caught exception when read a log message from log file: " + logFile, e);
      throw new LogFileReaderException("Cannot read a log message.", e);
    }
  }

  public void close() throws IOException {
    if (!closed) {
      textMessageReader.close();
      closed = true;
    }
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
      return textMessageReader.getByteOffset();
    } catch (Exception e) {
      LOG.error("Exception in getting textMessageReader byte offset of log file: " + logFile, e);
      throw new LogFileReaderException("Cannot get byte offset of the thrift textMessageReader", e);
    }
  }

  @Override
  public void setByteOffset(long byteOffset) throws LogFileReaderException {
    if (closed) {
      throw new LogFileReaderException("Reader closed.");
    }

    try {
      textMessageReader.setByteOffset(byteOffset);
    } catch (Exception e) {
      LOG.error("Caught exception when set textMessageReader byte offset of log file: " + logFile
          + " to: " + byteOffset, e);
      throw new LogFileReaderException("Can not set byte offset on the thrift textMessageReader",
          e);
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }
}
