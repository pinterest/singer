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

import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.client.logback.AppenderUtils;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

public class LogMessageEncoderTest {

  /**
   * Test thrift log file for seeking
   *
   * @throws Exception
   */
  @Test
  public void testWriteReadWithSeeks() throws Exception {

    File logFile = File.createTempFile("temp-thrift-log", ".tmp");
    String logFilePath = logFile.getAbsolutePath();
    try {
      OutputStream os = new BufferedOutputStream(new FileOutputStream(logFile));
      TTransport transport = new TFastFramedTransport(new TIOStreamTransport(os), 1000);
      TProtocol protocol = new TBinaryProtocol(transport);

      for (int i = 0; i < 10; i++) {
        LogMessage logMessage = new LogMessage()
            .setTimestampInNanos(System.currentTimeMillis() * 1000000)
            .setMessage(("sample message " + i).getBytes());
        logMessage.write(protocol);
        transport.flush();
      }
      transport.close();

      // read from the file now
      long offset = 0;
      for (int i = 0; i < 10; i++) {
        ThriftReader<LogMessage> thriftReader = new ThriftReader(
            logFilePath, new LogMessageFactory(), new BinaryProtocolFactory(), 1000, 1000);
        thriftReader.setByteOffset(offset);
        LogMessage logMessage = thriftReader.read();
        System.out.println("offset: " + offset + " logMessage: " + logMessage);
        offset = thriftReader.getByteOffset();
        thriftReader.close();
      }
    } finally {
      logFile.delete();
    }
  }

  /**
   * Test thrift log file created by LogMessageEncoder for seeking
   *
   * @throws Exception
   */
  @Test
  public void testEncodeReadWithSeeks() throws Exception {
    File logFile = File.createTempFile("temp-thrift-log", ".tmp");
    String logFilePath = logFile.getAbsolutePath();

    try {
      OutputStream os = new BufferedOutputStream(new FileOutputStream(logFile));
      AppenderUtils.LogMessageEncoder encoder = new AppenderUtils.LogMessageEncoder();
      encoder.init(os);

      for (int i = 0; i < 10; i++) {
        LogMessage logMessage = new LogMessage()
            .setTimestampInNanos(System.currentTimeMillis() * 1000000)
            .setMessage(("sample message " + i).getBytes());
        encoder.doEncode(logMessage);
      }
      encoder.close();

      // read from the file now
      long offset = 0;
      for (int i = 0; i < 10; i++) {
        ThriftReader<LogMessage> thriftReader = new ThriftReader(
            logFilePath, new LogMessageFactory(), new BinaryProtocolFactory(), 1000, 1000);
        thriftReader.setByteOffset(offset);
        LogMessage logMessage = thriftReader.read();
        System.out.println("offset: " + offset + " logMessage: " + logMessage);
        offset = thriftReader.getByteOffset();
        thriftReader.close();
      }
    } finally {
      logFile.delete();
    }
  }

  @Test
  public void createLogFiles() throws Exception {
    File logFile = new File("/tmp/thrift.log");
    OutputStream os = new BufferedOutputStream(new FileOutputStream(logFile));
    AppenderUtils.LogMessageEncoder encoder = new AppenderUtils.LogMessageEncoder();
    encoder.init(os);

    for (int i = 0; i < 10; i++) {
      LogMessage logMessage = new LogMessage()
          .setTimestampInNanos(System.currentTimeMillis() * 1000000)
          .setMessage(("sample message " + i).getBytes());
      encoder.doEncode(logMessage);
    }
    encoder.close();
  }

  // Factory that create LogMessage thrift objects.
  private static final class LogMessageFactory
      implements ThriftReader.TBaseFactory<com.pinterest.singer.thrift.LogMessage> {

    public com.pinterest.singer.thrift.LogMessage get() {
      return new com.pinterest.singer.thrift.LogMessage();
    }
  }

  private static final class BinaryProtocolFactory implements ThriftReader.TProtocolFactory {

    public TProtocol get(TTransport transport) {
      return new TBinaryProtocol(transport);
    }
  }
}
