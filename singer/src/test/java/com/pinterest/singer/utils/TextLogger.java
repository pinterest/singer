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


import com.google.common.base.Charsets;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A logger that logs text messages.
 */
public class TextLogger {

  private final static class ByteOffsetOutputStream extends FileOutputStream {

    private long byteOffset;

    public ByteOffsetOutputStream(String path) throws FileNotFoundException {
      super(path);
      byteOffset = 0;
    }

    @Override
    public void write(int i) throws IOException {
      super.write(i);
      byteOffset += 1;
    }

    @Override
    public void write(byte[] buf) throws IOException {
      super.write(buf);
      byteOffset += buf.length;
    }

    public long getByteOffset() {
      return byteOffset;
    }
  }

  private final ByteOffsetOutputStream byteOffsetOutputStream;

  public TextLogger(String path) throws FileNotFoundException {
    byteOffsetOutputStream = new ByteOffsetOutputStream(path);
  }

  public void logText(String text) throws IOException {
    ByteBuffer buf = Charsets.UTF_8.encode(text);
    byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes);
    byteOffsetOutputStream.write(bytes);
  }

  public long getByteOffset() {
    return byteOffsetOutputStream.getByteOffset();
  }
}
