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
package com.pinterest.singer.common;


import com.pinterest.singer.common.errors.LogStreamReaderException;
import com.pinterest.singer.thrift.LogMessageAndPosition;
import com.pinterest.singer.thrift.LogPosition;

import java.io.Closeable;

/**
 * Represent a random access LogStream reader which can read LogMessages from specified
 * LogPosition in a LogStream.
 */
public interface LogStreamReader extends Closeable {

  /**
   * @return the LogStream read by this LogStreamReader
   */
  LogStream getLogStream();

  /**
   * Read one LogMessage from the current read position.
   *
   * @return LogMessage and its position in LogStream. null if no more LogMessage in the LogStream.
   * @throws LogStreamReaderException when reader is closed or we can not read a LogMessage.
   */
  LogMessageAndPosition readLogMessageAndPosition()
      throws LogStreamReaderException;

  /**
   * @return the next read Position
   * @throws LogStreamReaderException when reader is closed.
   */
  LogPosition getPos() throws LogStreamReaderException;

  /**
   * Set the next read position to the specified LogPosition.
   *
   * @param position The position to be set.
   * @throws LogStreamReaderException when reader is closed or can not seek to the position.
   */
  void seek(LogPosition position) throws LogStreamReaderException;
}
