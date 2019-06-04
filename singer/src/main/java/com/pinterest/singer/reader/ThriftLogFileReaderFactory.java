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
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.LogFile;
import com.pinterest.singer.thrift.configuration.ThriftReaderConfig;
import com.pinterest.singer.utils.LogFileUtils;
import com.pinterest.singer.utils.SingerUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.FileSystems;

/**
 * Factory class that create ThriftLogFileReader instances based on ThriftReaderConfig.
 */
public class ThriftLogFileReaderFactory implements LogFileReaderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ThriftLogFileReaderFactory.class);
  private final ThriftReaderConfig readerConfig;

  public ThriftLogFileReaderFactory(ThriftReaderConfig readerConfig) {
    this.readerConfig = Preconditions.checkNotNull(readerConfig);
  }

  public LogFileReader getLogFileReader(LogStream logStream, LogFile logFile, String path, long byteOffset)
      throws Exception {
    LogFileReader reader;
    try {
      long inode = SingerUtils.getFileInode(FileSystems.getDefault().getPath(path));
      if (logFile.getInode() != inode) {
        // inode has changed that means the file was rotated for this path
        LOG.warn("Re-initialize log stream {} due to inode mismatch for {}: expect {}, is {}",
            logStream, path, logFile.getInode(), inode);
        logStream.initialize();
        path = logStream.getLogFilePath(logFile);
      }
      reader = new ThriftLogFileReader(logFile, path, byteOffset,
          readerConfig.getReaderBufferSize(), readerConfig.getMaxMessageSize());
    } catch (LogFileReaderException e) {
      LOG.warn("Exception in getLogFileReader", e);
      long inode = logFile.getInode();
      String newPath = LogFileUtils.getFilePathByInode(logStream, inode);
      if (newPath == null) {
        LOG.error("Logstream {} has no file for inode {}", logStream, inode);
        OpenTsdbMetricConverter.incr(SingerMetrics.FILE_LOOKUP_FAILURE, 1,
            "log=" + logStream.getLogStreamName(), "host=" + SingerUtils.getHostname());
        throw e;
      } else {
        LOG.warn("Logstream {}: found {} for inode {}",  logStream, newPath, inode);
        OpenTsdbMetricConverter.incr(SingerMetrics.FILE_LOOKUP_SUCCESS, 1,
            "log=" + logStream.getLogStreamName(), "host=" + SingerUtils.getHostname());
        reader = getLogFileReader(logStream, logFile, newPath, byteOffset);
      }
    }
    return reader;
  }
}
