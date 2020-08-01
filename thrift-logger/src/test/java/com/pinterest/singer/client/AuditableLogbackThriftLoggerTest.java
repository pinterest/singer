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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.pinterest.singer.client.logback.AppenderUtils;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.ThriftMessage;

import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ContextBase;
import com.twitter.io.TempDirectory;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.junit.jupiter.api.Test;

import java.util.zip.CRC32;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

public class AuditableLogbackThriftLoggerTest {

  private static String topicName = "topic1";

  private ThriftMessage[] messages = new ThriftMessage[100];

  public String initialize(String logFileDir, String topic, Class<?> thriftClazz) {
    File file = new File(logFileDir);
    if (file.exists()) {
      file.delete();
    }

    Appender<LogMessage> appender = null;
    try {
      appender = AppenderUtils.createFileRollingThriftAppender(
          new File(logFileDir), topic, 10000, new ContextBase(), 1);
      ThriftLogger logger = new AuditableLogbackThriftLogger(appender, topic, thriftClazz, true, 1.0);

      for (int i = 0; i < messages.length; i++) {
        ThriftMessage msg = new ThriftMessage();
        msg.setSequenceNum(i);
        msg.setPayload(("message" + i).getBytes());
        messages[i] = msg;
        logger.append(null, msg, System.nanoTime());
      }
      logger.close();
    } catch (Exception e) {
      e.printStackTrace();
      assert (false);
    }
    return logFileDir + "/" + topic;
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
    String testFilePath = initialize(logFilePath, topicName, ThriftMessage.class);

    RandomAccessFile tfile = null;
    TTransport transport = null;
    try {
      tfile = new RandomAccessFile(testFilePath, "r");
      InputStream is = new FileInputStream(tfile.getFD());
      transport = new TFastFramedTransport(new TIOStreamTransport(is), 10000);
      TProtocol protocol = new TBinaryProtocol(transport);
      TDeserializer der = new TDeserializer();
      LogMessage logMessage = new LogMessage();
      ThriftMessage thriftMessage = new ThriftMessage();
      CRC32 crc = new CRC32();

      for (int i = 0; i < messages.length; i++) {
        logMessage.read(protocol);
        der.deserialize(thriftMessage, logMessage.getMessage());
        assertArrayEquals(thriftMessage.getPayload(), messages[i].getPayload());
        assertEquals(thriftMessage.getSequenceNum(), messages[i].getSequenceNum());
        // same LoggingAuditHeaders object are attached to original ThriftMessage as well as to
        // LogMessage by AuditableLogbackThriftLogger. After deserialization, the headers should match.
        assertEquals(thriftMessage.getLoggingAuditHeaders(), logMessage.getLoggingAuditHeaders());
        // check if computed crc matches stored crc
        crc.reset();
        crc.update(logMessage.getMessage());
        assertEquals(crc.getValue(), logMessage.getChecksum());
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
    String testFilePath = initialize(logFilePath, topicName, ThriftMessage.class);
    TDeserializer der = new TDeserializer();
    CRC32 crc = new CRC32();

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
        ThriftMessage thriftMessage = new ThriftMessage();
        logMessage.read(protocol);
        der.deserialize(thriftMessage, logMessage.getMessage());
        assertArrayEquals(thriftMessage.getPayload(), messages[i].getPayload());
        assertEquals(thriftMessage.getSequenceNum(), messages[i].getSequenceNum());
        // check if computed crc matches stored crc
        crc.reset();
        crc.update(logMessage.getMessage());
        assertEquals(crc.getValue(), logMessage.getChecksum());
        startOffset = startOffset + transport.getBufferPosition() - transport.getBytesRemainingInBuffer() + 4;
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
