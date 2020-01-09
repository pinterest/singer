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
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.thrift.LogFile;
import com.pinterest.singer.thrift.configuration.TextReaderConfig;
import com.pinterest.singer.utils.LogFileUtils;
import com.pinterest.singer.utils.SingerUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.FileSystems;
import java.util.regex.Pattern;

/**
 * Factory class that create TextLogFileReader instances based on TextReaderConfig.
 */
public class TextLogFileReaderFactory implements LogFileReaderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TextLogFileReaderFactory.class);
  private final TextReaderConfig readerConfig;

  public TextLogFileReaderFactory(TextReaderConfig readerConfig) {
    this.readerConfig = Preconditions.checkNotNull(readerConfig);
  }

  @SuppressWarnings("resource")
  public LogFileReader getLogFileReader(
      LogStream logStream, LogFile logFile, String path, long byteOffset) throws Exception {
    LogFileReader reader;
    try {
      long inode = SingerUtils.getFileInode(FileSystems.getDefault().getPath(path));
      if (logFile.getInode() != inode) {
        LOG.warn("Re-initialize log stream {} due to inode mismatch: expect {}, is {}",
            logStream, logFile.getInode(), inode);
        logStream.initialize();
        path = logStream.getLogFilePath(logFile);
      }
      reader = new TextLogFileReader(logFile, path, byteOffset,
          readerConfig.getReaderBufferSize(),
          readerConfig.getMaxMessageSize(),
          readerConfig.getNumMessagesPerLogMessage(),
          Pattern.compile(readerConfig.getMessageStartRegex(), Pattern.UNIX_LINES),
          readerConfig.getTextLogMessageType(),
          readerConfig.isPrependTimestamp(),
          readerConfig.isPrependHostname(),
          SingerUtils.getHostNameBasedOnConfig(logStream, SingerSettings.getSingerConfig()),
          readerConfig.getPrependFieldDelimiter());
    } catch (LogFileReaderException e) {
      LOG.warn("Exception in getLogFileReader", e);
      long inode = logFile.getInode();
      String newPath = LogFileUtils.getFilePathByInode(logStream, inode);
      if (newPath == null) {
        LOG.error("{} has no file for inode {}", logStream, inode, e);
        throw e;
      } else {
        LOG.warn("In {}, found {} for inode {}", logStream, newPath, inode);
        reader = getLogFileReader(logStream, logFile, newPath, byteOffset);
      }
    }
    return reader;
  }
}