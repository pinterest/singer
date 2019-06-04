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
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

/**
 * Adapter class for RandomAccessFile so that we access it through InputStream interface.
 *
 * This class will allow us to set the file pointer when we need to go back in the stream. It
 * also allow us to track the byte offset of the next thrift message by reading the file pointer.
 */
final class RandomAccessFileInputStream extends InputStream {

  private final RandomAccessFile file;

  public RandomAccessFileInputStream(RandomAccessFile file) {
    this.file = Preconditions.checkNotNull(file);
  }

  @Override
  public int read() throws IOException {
    return file.read();
  }

  @Override
  public int read(byte[] buffer, int offset, int len) throws IOException {
    return file.read(buffer, offset, len);
  }

  @Override
  public int read(byte[] buffer) throws IOException {
    return file.read(buffer);
  }

  @Override
  public long skip(long n) throws IOException {
    return file.skipBytes(Ints.checkedCast(n));
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void close() throws IOException {
    file.close();
  }
}
