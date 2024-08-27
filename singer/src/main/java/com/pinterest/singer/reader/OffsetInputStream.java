package com.pinterest.singer.reader;

import java.io.IOException;

/**
 * Interface for InputStream that can track and set the byte offset for the next read.
 * byteOffset will start from the beginning of the file.
 */
public interface OffsetInputStream {
  long getByteOffset();
  void setByteOffset(long byteOffset) throws IOException;
  boolean isEOF() throws IOException;
}
