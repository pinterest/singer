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
package com.pinterest.singer.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.client.logback.AppenderUtils;

import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ContextBase;
import com.twitter.io.TempDirectory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import org.junit.jupiter.api.Test;

public class LogbackThriftLoggerTest {

  private static String testFileName = "test";
  private LogMessage[] messages = new LogMessage[10];

  public String initialize(String logFileDir, String logFile) {
    File file = new File(logFileDir);
    if (file.exists()) {
      file.delete();
    }

    Appender<LogMessage> appender = null;
    try {
      appender = AppenderUtils.createFileRollingThriftAppender(
          new File(logFileDir), logFile, 10, new ContextBase(), 1);
      ThriftLogger logger = new LogbackThriftLogger("test_topic", appender);

      for (int i = 0; i < messages.length; i++) {
        LogMessage msg = new LogMessage();
        msg.setMessage(("message" + i).getBytes()).setTimestampInNanos(System.nanoTime());
        messages[i] = msg;
        logger.append(null, msg.getMessage(), msg.getTimestampInNanos());
      }
      logger.close();
    } catch (Exception e) {
      e.printStackTrace();
      assert (false);
    }

    return logFileDir + "/" + logFile;
  }


  public void cleanup(String logFilePath) {
    File file = new File(logFilePath);
    if (file.exists()) {
      file.delete();
    }
  }

  /**
   * Test consecutive read of a log file that is created by the thrift logger
   * @throws IOException
   * @throws TException
   */
  @Test
  public void testLoggerWithConsecutiveReads() throws IOException, TException {
    File tmpDir = TempDirectory.create(true);
    String logFilePath = tmpDir.getPath();
    String testFilePath = initialize(logFilePath, testFileName);

    RandomAccessFile tfile = null;
    TTransport transport = null;
    try {
      tfile = new RandomAccessFile(testFilePath, "r");
      InputStream is = new FileInputStream(tfile.getFD());
      transport = new TFastFramedTransport(new TIOStreamTransport(is), 10000);
      TProtocol protocol = new TBinaryProtocol(transport);

      LogMessage logMessage = new LogMessage();
      for (int i = 0; i < messages.length; i++) {
        logMessage.read(protocol);
        assertEquals(logMessage, messages[i]);
      }
    } catch (IOException e) {
      e.printStackTrace();
      assert (false);
    } finally {
      if (transport != null) {
        transport.close();
      }
      if (tfile != null) {
        tfile.close();
      }
    }

    cleanup(logFilePath);

  }

  /**
   * Test read a log file that the thrift-logger creates by starting from offsets.
   * TFramedTransport.flush() writes the frame size before writing the actual message.
   * We use 'startOffset + transport.getBufferPosition() - transport.getBytesRemainingInBuffer()
   * + 4'
   * to get the right offset for the next frame.
   * https://github.com/apache/thrift/blob/master/lib/java/src/org/apache/thrift/transport
   * /TFramedTransport.java#L150
   *
   * @throws IOException
   */
  @Test
  public void testLoggerWithSeekReads() throws IOException {
    File tmpDir = TempDirectory.create(true);
    String logFilePath = tmpDir.getPath();
    String testFilePath = initialize(logFilePath, testFileName);

    long startOffset = 0;
    for (int i = 0; i < messages.length; i++) {
      RandomAccessFile tfile = null;
      TTransport transport = null;
      try {
        tfile = new RandomAccessFile(testFilePath, "r");
        tfile.seek(startOffset);
        InputStream is = new FileInputStream(tfile.getFD());
        transport = new TFastFramedTransport(new TIOStreamTransport(is), 10000);
        TProtocol protocol = new TBinaryProtocol(transport);
        LogMessage logMessage = new LogMessage();
        logMessage.read(protocol);

        int bufferPos = transport.getBufferPosition();
        int bytesRemainingInBuffer = transport.getBytesRemainingInBuffer();
        System.out.println("startOffset = " + startOffset + "bufferPos = " + bufferPos +
            " bytesRemainingInBuffer = " + bytesRemainingInBuffer);

        assertEquals(logMessage, messages[i]);
        startOffset =
            startOffset + transport.getBufferPosition() - transport.getBytesRemainingInBuffer() + 4;
      } catch (Exception e) {
        e.printStackTrace();
        assert (false);
      } finally {
        if (transport != null) {
          transport.close();
        }
        if (tfile != null) {
          tfile.close();
        }
      }
    }
    cleanup(logFilePath);
  }
}
