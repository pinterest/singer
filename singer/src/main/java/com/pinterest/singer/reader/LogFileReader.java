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

import com.pinterest.singer.thrift.LogFile;
import com.pinterest.singer.thrift.LogMessageAndPosition;

import java.io.Closeable;

/**
 * Represent a LogFile reader from which users can read LogMessage.
 */
public interface LogFileReader extends Closeable {

  /**
   * Read a LogMessage with its position.
   *
   * @return LogMessage and its position in the LogStream. Return null if we reach the end of the
   * LogFile.
   * @throws LogFileReaderException when reader is closed or we can not read a LogMessage from the
   *                                reader.
   */
  LogMessageAndPosition readLogMessageAndPosition() throws LogFileReaderException;

  /**
   * Return the LogFile that will be read from.
   *
   * @return LogFile that is being read.
   * @throws LogFileReaderException when reader is closed.
   */
  LogFile getLogFile() throws LogFileReaderException;

  /**
   * Get the byte offset of the next LogMessage that will be read from the LogFile
   *
   * @return byte offset of the next LogMessage that will be read from the LogFile
   * @throws LogFileReaderException when reader is closed.
   */
  long getByteOffset() throws LogFileReaderException;

  /**
   * Set byte offset of the next LogMessage that will be read from the LogFile.
   *
   * @param byteOffset byte offset to be set.
   * @throws LogFileReaderException when reader is closed or can not seek to the byte offset.
   */
  void setByteOffset(long byteOffset) throws LogFileReaderException;

  /**
   *
   * @return if it is closed.
   */
  boolean isClosed();
}
