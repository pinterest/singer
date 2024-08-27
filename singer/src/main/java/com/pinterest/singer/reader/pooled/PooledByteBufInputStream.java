/**
 * Copyright 2024 Pinterest, Inc.
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
package com.pinterest.singer.reader.pooled;

import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.reader.OffsetInputStream;
import com.pinterest.singer.reader.RandomAccessFileInputStream;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

/**
 * This is a BufferedInputStream like implementation that uses a Netty's Pooled ByteBuf instead.
 * It can track and set the byte offset for the next read. The byteOffset will start from the
 * beginning of the file. Note that the file can be appended as it is being read.Most of the methods
 * in this class are based on ByteOffsetInputStream and BufferedInputStream.
 */
public class PooledByteBufInputStream extends InputStream implements OffsetInputStream {

  private ByteBuf buffer;
  private InputStream source;
  private long byteOffset;
  private final RandomAccessFile randomAccessFile;

  public PooledByteBufInputStream(RandomAccessFile randomAccessFile, ByteBuf byteBuf) {
    source = new RandomAccessFileInputStream(Preconditions.checkNotNull(randomAccessFile));
    buffer = Preconditions.checkNotNull(byteBuf);
    OpenTsdbMetricConverter.incr("singer.reader.pooled.create_buffer");
    this.randomAccessFile = randomAccessFile;
    byteOffset = 0;
  }

  /**
   * Reads a single byte from the buffer. If the buffer is empty, it will refill the buffer.
   *
   * @return the byte read or -1 if the end of the stream has been reached.
   * @throws IOException
   */
  @Override
  public synchronized int read() throws IOException {
    if(!buffer.isReadable()) {
      refillBuffer();
    }
    return buffer.isReadable() ? buffer.readByte() & 0xFF : -1;
  }

  /**
   * Refills the buffer with data from the source.
   *
   * @throws IOException
   */
  private void refillBuffer() throws IOException {
    buffer.clear();
    int bytesToTransfer = buffer.capacity();
    // read directly from the source into buffer with writeBytes to avoid extra copy
    buffer.writeBytes(source, bytesToTransfer);
  }

  /**
   * Basic method to read data into a byte array. It will attempt
   * to read from the buffer first and then from the source if necessary.
   *
   * @param b     destination buffer
   * @param off   the start offset in array <code>b</code>
   *                   at which the data is written.
   * @param len   the maximum number of bytes to read.
   * @return The total number of bytes read.
   * @throws IOException
   */
  @Override
  public synchronized int read(byte b[], int off, int len) throws IOException {
    int bytesRead = readIntoBuffer(b, off, len);
    byteOffset += Math.max(0, bytesRead);
    return bytesRead;
  }

  /**
   * Reads data from the buffer or source until the number of bytes requested is read or
   * the end of the stream is reached. This method repeatedly calls readAndFill until either
   * of the conditions are met.
   *
   * @param b destination buffer
   * @param off the start offset in array <code>b</code> at which the data is written.
   * @param len
   * @return
   * @throws IOException
   */
  private synchronized int readIntoBuffer(byte b[], int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    // Read until we read the number of bytes requested or until we run out of bytes to read.
    int totalBytesRead = 0;
    while (true) {
      int bytesRead = readAndFill(b, off + totalBytesRead, len - totalBytesRead);
      if (bytesRead <= 0)
        return (totalBytesRead == 0) ? bytesRead : totalBytesRead;
      totalBytesRead += bytesRead;
      if (totalBytesRead >= len)
        return totalBytesRead;
      // Return if there are no more available bytes in the source.
      if (source != null && source.available() <= 0)
        return totalBytesRead;
    }
  }

  /**
   * Reads data from the buffer into a byte array and refills
   * the buffer from the source if necessary.
   *
   * @param b destination buffer
   * @param off the start offset in array <code>b</code at which the data is written.
   * @param len length of the data to read
   * @return the number of bytes read
   * @throws IOException
   */
  private int readAndFill(byte[] b, int off, int len) throws IOException {
    int avail = buffer.readableBytes();
    if (avail <= 0) {
      // Read directly from the source if the length is greater than the buffer capacity.
      if (len >= buffer.capacity()) {
        return source.read(b, off, len);
      }
      refillBuffer();
      avail = buffer.readableBytes();
      if (avail <= 0) return -1;
    }
    int cnt = (avail < len) ? avail : len;
    buffer.getBytes(buffer.readerIndex(), b, off, cnt);
    buffer.readerIndex(buffer.readerIndex() + cnt);
    return cnt;
  }

  /**
   * Reads directly from source.
   *
   * @param buffer destination buffer
   * @return number of bytes read
   * @throws IOException
   */
  @Override
  public int read(byte[] buffer) throws IOException {
    return source.read(buffer);
  }

  /**
   * Get the byte offset for the next read.
   */
  public long getByteOffset() {
    return byteOffset;
  }

  /**
   * Set the byte offset for the next read.
   */
  public void setByteOffset(long byteOffset) throws IOException {
    // Clear the buffer
    buffer.readerIndex(buffer.writerIndex());
    // Set file byte offset
    randomAccessFile.seek(byteOffset);
    // Set stream byte offset.
    this.byteOffset = byteOffset;
  }

  /**
   * Checks if the stream is at the end of the file.
   *
   * @return true if the stream is at the end of the file.
   * @throws IOException
   */
  public boolean isEOF() throws IOException {
    // If buffer is not empty, we are not at end of stream.
    if (buffer.readerIndex() != buffer.writerIndex()) {
      return false;
    }
    // Try read one byte from the file.
    long currentFilePointer = randomAccessFile.getFilePointer();
    if (randomAccessFile.read() == -1) {
      // We can not read one byte. File pointer will not be moved by the read. Just return true.
      return true;
    } else {
      // We can read one byte. Move file pointer back and return false.
      randomAccessFile.seek(currentFilePointer);
      return false;
    }
  }


  /**
   * Skips over and discards n bytes of data from the input stream. In addition, it
   * will not skip more bytes than the number of bytes available in the buffer.
   *
   * @param n the number of bytes to be skipped.
   * @return number of bytes skipped
   * @throws IOException
   */
  @Override
  public long skip(long n) throws IOException {
    // We pose a limit on the number of bytes that can be skipped since
    // the ByteBuf methods can only take integers.
    if (!(Integer.MIN_VALUE <= n && n <= Integer.MAX_VALUE)) {
      throw new IllegalArgumentException("Skip value is too large");
    }

    long bytesSkipped = 0;
    if (n <= 0) {
      return 0;
    }
    int readableBytes = buffer.readableBytes();
    if (readableBytes <= 0) {
      refillBuffer();
      readableBytes = buffer.readableBytes();
    }
    bytesSkipped = (readableBytes < n) ? readableBytes : n;
    // Skip bytes by moving the reader index.
    buffer.readerIndex((int) bytesSkipped);
    byteOffset += bytesSkipped;
    return bytesSkipped;
  }

  /**
   * Closes the stream and releases the buffer.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    source.close();
    buffer.release();
    OpenTsdbMetricConverter.incr("singer.reader.pooled.close_buffer");
  }
}