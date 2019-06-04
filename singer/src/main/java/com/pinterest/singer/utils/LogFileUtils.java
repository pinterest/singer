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

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.thrift.configuration.FileNameMatchMode;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Utility class for LogFile
 */
public final class LogFileUtils {

  private static final Logger LOG = LoggerFactory.getLogger(LogFileUtils.class);

  // No instance.
  private LogFileUtils() {
  }

  public static String getFilePathByInode(LogStream logStream, long inode)
      throws IOException {
    String result = null;
    String logDir = logStream.getLogDir();
    File dir = new File(logDir);
    if (dir.exists()) {
      SingerLogConfig logConfig = logStream.getSingerLog().getSingerLogConfig();
      String regexStr = logStream.getFileNamePrefix();
      if (logConfig.getFilenameMatchMode() == FileNameMatchMode.PREFIX) {
        regexStr += ".*";
      }
      LOG.info("Matching files under {} with filter {}", logDir, regexStr);
      FileFilter fileFilter = new RegexFileFilter(regexStr);
      File[] files = dir.listFiles(fileFilter);
      for (File file : files) {
        String path = file.getAbsolutePath();
        if (inode == SingerUtils.getFileInode(path)) {
          result = path;
          break;
        }
      }
    }
    return result;
  }
}
