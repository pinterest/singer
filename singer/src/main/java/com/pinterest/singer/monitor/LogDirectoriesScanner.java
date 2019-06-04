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
package com.pinterest.singer.monitor;


import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerLog;
import com.pinterest.singer.thrift.LogFile;
import com.pinterest.singer.utils.SingerUtils;

import com.google.common.collect.Ordering;
import java.util.ArrayList;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class scan the log directory, and correct mismatch if there is any
 */
public class LogDirectoriesScanner implements  Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(LogDirectoriesScanner.class);
  private Thread thread;
  private Set<Path> monitoredPaths;

  public LogDirectoriesScanner(Set<Path> path) {
    monitoredPaths = new HashSet<>(path);
  }

  public void start() {
    thread = new Thread(this);
    thread.start();
  }

  public void join() throws InterruptedException {
    thread.join();
  }

  public void stop() {
    thread.interrupt();
  }

  public List<Path> getMonitoredPaths() {
    List<Path> result = new ArrayList();
    result.addAll(monitoredPaths);
    return result;
  }

  public List<File> getFilesInSortedOrder(Path dirPath) {
    File[] files = dirPath.toFile().listFiles((FileFilter) FileFileFilter.FILE);

    // Sort the file first by last_modified timestamp and then by name in case two files have
    // the same mtime due to precision (mtime is up to seconds).
    Ordering ordering = Ordering.from(new SingerUtils.LogFileComparator());
    List<File> logFiles = ordering.sortedCopy(Arrays.asList(files));
    return logFiles;
  }

  public void updateFilesInfo(Path dirPath, List<File> files) {
    for (File file : files) {
      updateFileInodeInfo(dirPath, file);
    }
  }
  /**
   * Re-syncs the map of directory to file and inode names with the current state of the disk.
   * As some new files might have been created, we need to create new LogStream for files that
   * do not belong to any existing log streams, but matches the regex of some singer
   * configurations.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void run() {
    LOG.info("Start updating inode info for all log directories");
    for (Path dirPath : monitoredPaths) {
      List<File> logFiles = getFilesInSortedOrder(dirPath);
      updateFilesInfo(dirPath, logFiles);
    }
    LOG.info("Finish updating inode info");
  }

  /**
   * Update the inode info for a single file. The basic logic is as follows:
   *   1. check each logstreams under the same log directory to see if the file matches
   *   2. If the file matches an existing log stream, include the file's info in that logstream
   *   3. Otherwise, we got through all singer configurations to see if it matches any logstream
   *      regex in singur configurations. Create a new logstream if the file matches one of the
   *      singer configuration.
   */
  private void updateFileInodeInfo(Path dirPath, File logFile) {
    try {
      boolean foundMatchingStream = false;
      Collection<LogStream> logStreams = LogStreamManager.getLogStreams(dirPath);
      if (logStreams != null) {
        for (LogStream stream : logStreams) {
          String fileAbsolutePath = logFile.getAbsolutePath();
          if (stream.acceptFile(fileAbsolutePath) && logFile.exists()) {
            Path filePath = logFile.toPath();
            long inode = SingerUtils.getFileInode(filePath);
            stream.put(new LogFile(inode), filePath.toString());
            foundMatchingStream = true;
          }
        }
      }
      // if there is no existing log streams that matches the file, check if there
      // is any existing configurations that matches the file. create a new log stream
      // in this case.
      if (!foundMatchingStream) {
        List<SingerLog> singerLogs = LogStreamManager.getMatchedSingerLogs(dirPath, logFile);
        for (SingerLog singerLog : singerLogs) {
          LOG.info("creating logstream for {}:{}", singerLog.getLogName(), logFile);
          LogStreamManager.getInstance().createLogStream(singerLog, logFile.toPath().toAbsolutePath());
        }
      }
    } catch (NoSuchFileException e) {
      // the files under one directory can be delted after we call File.listFiles() to
      // get the list of files. In this case, we shall ignore the deleted files.
      LOG.info("File {} has been deleted.", logFile);
    } catch (Exception e) {
      LOG.error("Caught an exception while updating inode info for {}", logFile, e);
    }
  }
}
