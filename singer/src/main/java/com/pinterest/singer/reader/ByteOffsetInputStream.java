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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Buffered InputStream implementation which can track and set the byte offset for the next read.
 * The byteOffset will be from the beginning of the file.
 * Note that the file can be appended as it is being read.
 */
public final class ByteOffsetInputStream extends BufferedInputStream {

  private final RandomAccessFile randomAccessFile;
  // Byte offset of current read position.
  private long byteOffset;

  public ByteOffsetInputStream(RandomAccessFile randomAccessFile,
                               int readBufferSize) throws IOException {
    super(new RandomAccessFileInputStream(Preconditions.checkNotNull(randomAccessFile)),
        readBufferSize);
    this.randomAccessFile = randomAccessFile;
    this.byteOffset = 0;
  }

  public long getByteOffset() {
    return byteOffset;
  }

  public void setByteOffset(long byteOffset) throws IOException {
    // Clear the buffer
    pos = count;
    // Set file byte offset
    randomAccessFile.seek(byteOffset);
    // Set stream byte offset.
    this.byteOffset = byteOffset;
  }

  public boolean isEOF() throws IOException {
    // If buffer is not empty, we are not at end of stream.
    if (pos != count) {
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

  @Override
  public int read() throws IOException {
    int i = super.read();
    if (i != -1) {
      ++byteOffset;
    }
    return i;
  }

  @Override
  public int read(byte buffer[], int offset, int len) throws IOException {
    int bytesRead = super.read(buffer, offset, len);
    byteOffset += Math.max(0, bytesRead);
    return bytesRead;
  }

  @Override
  public int read(byte[] buffer) throws IOException {
    int bytesRead = super.read(buffer);
    byteOffset += bytesRead;
    return bytesRead;
  }

  @Override
  public long skip(long n) throws IOException {
    long bytesSkipped = super.skip(n);
    byteOffset += bytesSkipped;
    return bytesSkipped;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}
