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

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.thrift.LogFile;

/**
 * Factory to get LogFileReader implementation instances.
 */
public interface LogFileReaderFactory {

  /**
   * Get LogFileReader for a LogFile.
   *
   * @param logFile    LogFile to read from.
   * @param path       path of the LogFile in the filesystem.
   * @param byteOffset byte offset to the beginning of the LogFile to read from.
   * @return LogFileReader created for the LogFile.
   * @throws Exception when fail to get a reader for the LogFile.
   */
  LogFileReader getLogFileReader(
      LogStream logStream, LogFile logFile, String path, long byteOffset) throws Exception;
}
