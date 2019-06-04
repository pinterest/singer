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
package com.pinterest.singer.utils;

import com.pinterest.singer.thrift.LogFile;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;

/**
 * Thrift Logger which is used to log thrift log messages into a log file.
 *
 * This class is used by unit test and ThriftLogGenerator to write log messages to thrift log
 * file. It provides APIs for client to control the flush behavior and log file rotation.
 */
public final class SimpleThriftLogger<T extends TBase> implements Closeable {

  private static final class ByteOffsetTFramedTransport extends TFramedTransport {

    private long byteOffset;

    public ByteOffsetTFramedTransport(TTransport transport) {
      super(transport);
      byteOffset = 0;
    }

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
      super.write(buf, off, len);
      byteOffset += len;
    }

    @Override
    public void flush() throws TTransportException {
      super.flush();
      // Add 4 bytes for the frame size.
      byteOffset += 4;
    }

    public long getByteOffset() {
      return byteOffset;
    }
  }

  private final String fileName;

  private BufferedOutputStream bufferedOutputStream;
  private ByteOffsetTFramedTransport transport;
  private TProtocol protocol;

  private long byteOffset;

  public SimpleThriftLogger(String filename) throws Exception {
    this.fileName = filename;
    bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(fileName, true));
    transport = new ByteOffsetTFramedTransport(new TIOStreamTransport(bufferedOutputStream));
    protocol = new TBinaryProtocol(transport);
    byteOffset = 0;
  }

  /**
   * Write a thrift message to log file.
   * @param message to be written
   * @throws Exception on write error.
   */
  public void logThrift(T message) throws Exception {
    message.write(protocol);
    // Flush to make sure one message per frame.
    transport.flush();
  }

  /**
   * Simple implementation of log file rotation.
   * @throws java.io.IOException
   */
  public void rotate() throws IOException {
    close();

    int i = 0;
    while (new File(String.format("%s.%d", fileName, ++i)).exists()) {
      ;
    }

    for (int j = i - 1; j >= 1; --j) {
      FileUtils.moveFile(
          new File(String.format("%s.%d", fileName, j)),
          new File(String.format("%s.%d", fileName, j + 1)));
    }
    FileUtils.moveFile(new File(fileName), new File(fileName + ".1"));
    bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(fileName, true));
    transport = new ByteOffsetTFramedTransport(new TIOStreamTransport(bufferedOutputStream));
    protocol = new TBinaryProtocol(transport);
  }

  public void flush() throws IOException {
    bufferedOutputStream.flush();
  }

  public LogFile getLogFile() throws IOException {
    long inode = SingerUtils.getFileInode(FileSystems.getDefault().getPath(fileName));
    return new LogFile(inode);
  }

  public long getByteOffset() {
    return transport.getByteOffset();
  }

  public void close() throws IOException {
    transport.close();
  }
}
