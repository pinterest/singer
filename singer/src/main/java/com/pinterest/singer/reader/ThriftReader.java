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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Reader that reads Thrift messages of thrift type from a file
 * <p/>
 * This class is NOT thread-safe.
 */
public class ThriftReader<T extends TBase> implements Closeable {

  /**
   * Factory that get a TBase instance of the thrift type to be read.
   *
   * @param <T> The thrift message type to be read.
   */
  public static interface TBaseFactory<T> {

    T get();
  }

  /**
   * Factory that get a TProtocol instance.
   */
  public static interface TProtocolFactory {

    TProtocol get(TTransport transport);
  }

  // Factory that creates empty objects that will be initialized with values from the file.
  private final TBaseFactory<T> baseFactory;

  // The ByteOffsetInputStream to read from.
  private final ByteOffsetInputStream byteOffsetInputStream;

  // The framed framedTransport.
  private final TFramedTransport framedTransport;

  // TProtocol implementation.
  private final TProtocol protocol;

  private final int readBufferSize;

  private final int maxMessageSize;

  public ThriftReader(
      String path,
      TBaseFactory<T> baseFactory,
      TProtocolFactory protocolFactory,
      int readBufferSize,
      int maxMessageSize) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
    Preconditions.checkNotNull(protocolFactory);

    this.byteOffsetInputStream = new ByteOffsetInputStream(
        new RandomAccessFile(path, "r"), readBufferSize);
    this.framedTransport = new TFramedTransport(new TIOStreamTransport(this
        .byteOffsetInputStream), maxMessageSize);
    this.baseFactory = Preconditions.checkNotNull(baseFactory);
    this.protocol = protocolFactory.get(this.framedTransport);

    this.readBufferSize = readBufferSize;
    this.maxMessageSize = maxMessageSize;
  }

  /**
   * Read one thrift message.
   *
   * @return next thrift message from the reader. null if no thrift message in the reader.
   * @throws IOException when file error.
   * @throws TException  when parse error.
   */
  public T read() throws IOException, TException {
    // If frame buffer is empty and we are at EOF of underlying input stream, return null.
    if (framedTransport.getBytesRemainingInBuffer() == 0 && byteOffsetInputStream.isEOF()) {
      return null;
    }

    T t = baseFactory.get();
    t.read(protocol);
    return t;
  }

  /**
   * @return byte offset of the next message.
   * @throws IOException on file error.
   */
  public long getByteOffset() throws IOException {
    Preconditions.checkState(
        byteOffsetInputStream.getByteOffset() >= framedTransport.getBytesRemainingInBuffer());
    return byteOffsetInputStream.getByteOffset() - framedTransport.getBytesRemainingInBuffer();
  }

  /**
   * Set byte offset of the next message to be read.
   *
   * @param byteOffset byte offset.
   * @throws IOException on file error.
   */
  public void setByteOffset(long byteOffset) throws IOException {
    // If we already at the byte offset, return.
    if (getByteOffset() == byteOffset) {
      return;
    }

    // Clear the buffer
    framedTransport.consumeBuffer(framedTransport.getBytesRemainingInBuffer());

    // Set underlying stream byte offset
    byteOffsetInputStream.setByteOffset(byteOffset);
  }

  /**
   * Close the reader.
   *
   * @throws IOException on file error.
   */
  public void close() throws IOException {
    framedTransport.close();
  }
}
