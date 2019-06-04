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

import com.google.common.base.MoreObjects;
import com.pinterest.singer.common.errors.LogStreamException;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.LogFile;
import com.pinterest.singer.thrift.LogFileAndPath;
import com.pinterest.singer.thrift.configuration.FileNameMatchMode;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.utils.SingerUtils;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.comparator.CompositeFileComparator;
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.apache.commons.io.comparator.NameFileComparator;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represent a log stream which logger append log message onto.
 *
 * A log stream can span over a sequence of log files created by log rotation.
 * The assumptions about a log stream are:
 *    1. Logger or third-party agent will rotate the log file.
 *    2. As soon as a log file is rotated, the log file will be sealed. No new update will be applied to the
 *       rotated log file. That is, its last_modified timestamp stays unchanged.
 *    3. Log file with older last_modified timestamp will contain older message than log file with
 *       newer last_modified timestamp.
 *
 * The log file rotation is independent of LogStream processing. Because of this, inode -> file
 * mapping may change during LogStream refresh and processing. For the inode -> file mapping
 * change that happens during log stream refresh, we use retry to get a stable mapping.
 * During the stream processing, if inode -> file mapping changes, we will find out the latest
 * file name for the inode using LogFileUtils.getFilePathByInode method.
 *
 * A LogStream object can be concurrently accessed by:
 *    1) LogStreamManager and FileSystemMonitor for updating LogStream related info
 *    2) LogStreamProcessor for looking up file path from inode info
 *    3) LogDirectoriesScanner for keeping LogStream info  consistent after file system event overflow
 *
 * We need to use synchronization primitive to guarantee mutual exclusive access between
 * readers and writers.
 *
 */
public class LogStream {

  private static final Logger LOG = LoggerFactory.getLogger(LogStream.class);

  private static final int MAX_RETRY = 3;

  // The Log this LogStream is part of.
  private final SingerLog singerLog;

  // The logStreamName of this LogStream within the SingerLog.
  // This will be the common prefix of all LogFiles in this LogStream.
  // It is unique within the SingerLog instance.
  private final String logStreamName;

  private String fileNamePrefix;

  // The full-path prefix for finding the log files of the stream
  private String fullPathPrefix;

  // The descriptor of the LogStream which consists of SingleLog name and LogStream name.
  // It is globally unique descriptor of the LogStream.
  private final String logStreamDescriptor;

  private Object logFilesInfoLock = new Object();

  // The current sequence of LogFile paths in filesystem in the same order as logFiles.
  // This sequence of paths can get out of sync with the filesystem if LogFiles are rotated
  // after the LogStream is refreshed.
  private List<LogFileAndPath> logFilePaths;

  // Store [LogFile -> logFilePath index] map
  private Map<LogFile, Integer> logFilePathsIndex;

  // when the logstream is created
  private long creationTime = -1L;

  // The timestamp of the last processed message in milliseconds
  private long latestProcessedMessageTime = -1L;

  // the latest modification time for the log stream in milliseconds
  private long lastStreamModificationTime;

  private long lastCompleteCycleTime;

  private FileNameMatchMode fileNameMatchMode;

  public LogStream(SingerLog singerLog, String fileNamePrefix) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(fileNamePrefix));

    this.singerLog = Preconditions.checkNotNull(singerLog);
    this.logStreamName = fileNamePrefix;
    this.logStreamDescriptor = String.format("%s:%s", singerLog.getLogName(), fileNamePrefix);
    this.logFilePaths = Lists.newArrayList();
    this.logFilePathsIndex = new HashMap<>();
    this.lastStreamModificationTime = -1L;

    String dir = this.singerLog.getSingerLogConfig().getLogDir();
    this.fileNameMatchMode = singerLog.getSingerLogConfig().getFilenameMatchMode();
    this.fileNamePrefix = fileNamePrefix;
    this.fullPathPrefix = FilenameUtils.concat(dir, fileNamePrefix);
    this.creationTime = System.currentTimeMillis();
  }

  /**
   * @return the number of files in the log stream
   */
  public int size() {
    return logFilePaths.size();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void initialize() throws IOException {
    SingerLogConfig singerLogConfig = singerLog.getSingerLogConfig();
    String regexStr = fileNamePrefix;
    File logDir = new File(singerLogConfig.getLogDir());

    if (singerLogConfig.getFilenameMatchMode() == FileNameMatchMode.PREFIX) {
      regexStr += ".*";
    }

    LOG.info("Matching files under {} with filter {}", logDir, regexStr);
    FileFilter fileFilter = new RegexFileFilter(regexStr);
    File[] files = logDir.listFiles(fileFilter);

    // Sort the file first by last_modified timestamp and then by name in case two files have
    // the same mtime due to precision (mtime is up to seconds).
    Ordering ordering = Ordering.from(
        new CompositeFileComparator(
            LastModifiedFileComparator.LASTMODIFIED_COMPARATOR, NameFileComparator.NAME_REVERSE));
    List<File> logFiles = ordering.sortedCopy(Arrays.asList(files));

    LOG.info(files.length + " files matches the regex '{}'", regexStr);
    synchronized (logFilesInfoLock) {
      logFilePaths.clear();
      logFilePathsIndex.clear();
      for (File entry : logFiles) {
        long inode = SingerUtils.getFileInode(entry.toPath());
        append(new LogFile(inode), entry.toPath().toString());
      }
    }
    OpenTsdbMetricConverter.incr(SingerMetrics.LOGSTREAM_INITIALIZE, 1,
        "log=" + logStreamName, "host=" + SingerUtils.getHostname());
  }

  public boolean containsFile(String fileName) {
    boolean result = false;
    synchronized (logFilesInfoLock) {
      for (int i = 0; i < logFilePaths.size(); i++) {
        try {
          LogFileAndPath logFileAndPath = logFilePaths.get(i);
          File file = new File(logFileAndPath.path);

          if (fileName.equals(file.getName())) {
            result = true;
            break;
          }
        } catch (Exception e) {
          LOG.error("Exception in checking file existence ", e);
        }
      }
    }
    return result;
  }

  /**
   * @return a snapshot of the LogFileAndPath list
   */
  public List<LogFileAndPath> getLogFileAndPaths() {
    synchronized (logFilesInfoLock) {
      return Lists.newArrayList(logFilePaths);
    }
  }

  public long getInodeByFileName(String fileName) {
    synchronized (logFilesInfoLock) {
      for (int i = 0; i < logFilePaths.size(); i++) {
        LogFileAndPath logFileAndPath = logFilePaths.get(i);
        File file = new File(logFileAndPath.getPath());
        if (file.getName().equals(fileName)) {
          return logFileAndPath.logFile.getInode();
        }
      }
      return -1;
    }
  }

  public long getCreationTime() {
    return creationTime;
  }

  /**
   * @return the SingerLog this LogStream is part of.
   */
  public SingerLog getSingerLog() {
    return singerLog;
  }

  /**
   * @return the name of this LogStream in the SingerLog.
   */
  public String getLogStreamName() {
    return logStreamName;
  }

  /**
   * @return the file name matching prefix for the logstream
   */
  public String getFileNamePrefix() {
    return fileNamePrefix;
  }

  public String getFullPathPrefix() {
    return fullPathPrefix;
  }

  /**
   * @return Get global unique descriptor of the LogStream.
   */
  public String getLogStreamDescriptor() {
    return logStreamDescriptor;
  }

  public long getLastStreamModificationTime() {
    synchronized (logFilesInfoLock) {
      if (logFilePaths.size() > 0) {
        try {
          LogFileAndPath lastFileAndPath = logFilePaths.get(logFilePaths.size() - 1);
          File file = new File(lastFileAndPath.getPath());
          lastStreamModificationTime = file.lastModified();
        } catch (Exception e) {
          LOG.error("Failed to get latest stream modification time", e);
        }
      }
    }
    return lastStreamModificationTime;
  }

  public long getLatestProcessedMessageTime() {
    return latestProcessedMessageTime;
  }

  public void setLatestProcessedMessageTime(long timestamp) {
    this.latestProcessedMessageTime = timestamp;
  }

  public String getLogDir() {
    return this.singerLog.getSingerLogConfig().getLogDir();
  }

  /**
   * Check whether the file name matches the log stream. This is used by
   * FileSystemMonitor to add file to a log stream
   * @param fileAbsolutePath  a file name
   * @return true or false
   */
  public boolean acceptFile(String fileAbsolutePath) {
    switch (fileNameMatchMode) {
      case EXACT:
        return fileAbsolutePath.equals(fullPathPrefix);
      case PREFIX:
        return fileAbsolutePath.startsWith(fullPathPrefix);
      default:
        return false;
    }
  }

  /**
   * @return first LogFile in the log stream. Currently, this is the LogFile with oldest
   * mtime in the LogStream
   */
  public LogFileAndPath getFirstLogFile() {
    synchronized (logFilesInfoLock) {
      return isEmpty() ? null : logFilePaths.get(0);
    }
  }

  /**
   * @return the last LogFile in the log stream. It is the LogFile with the latest mtime.
   */
  public LogFileAndPath getLatestLogFileAndPath() {
    LogFileAndPath result = null;
    synchronized (logFilesInfoLock) {
      if (!isEmpty()) {
        int index = logFilePaths.size() - 1;
        result = logFilePaths.get(index);
      }
    }
    return result;
  }


  /**
   * Find the position of the file in the log stream. Note that we need to keep the invariant
   * that the files in logFilePaths are put in the ascending order by last modified time.
   * If two files are of the same last modified time, they are sorted by file name in descending order.
   *
   * @param path  full path of the log file that we want to add to the stream
   * @param lastModifiedTime last modified time in milliseconds for the log file
   * @return  index of the list that we can insert file @path to keep  @logFilePaths in sorted order
   */
  private int findPositionInStream(String path, long lastModifiedTime) {
    int pos = 0;
    long timestamp;
    // iterate through logFilePaths list to find the right position to insert the log file into
    while (pos < logFilePaths.size()) {
      LogFileAndPath lfp = logFilePaths.get(pos);
      timestamp = SingerUtils.getFileLastModifiedTime(lfp.getPath());
      if (lastModifiedTime > timestamp) {
        // move to the next index if lastModifiedTime is at a later time
        pos++;
        continue;
      } else if (lastModifiedTime == timestamp) {
        String anotherPath = lfp.getPath();
        if (anotherPath.length() > path.length() ||
            (anotherPath.length() == path.length() && anotherPath.compareTo(path) > 0)) {
          // move to the next index if lastModifiedTime equals, but @lfp has
          // a longer name, or @lfp file name is larger in the alphabetic order.
          pos++;
          continue;
        }
      }
      break;
    }
    return pos;
  }


  /**
   *  Put the LogFilePath to the proper position in the log stream. Note that we need to
   *  maintain the invariant that the files in @logFilePaths are ordered in ascending order
   *  by the last modification time.
   *
   * @param logFile  the log file object that has inode info
   * @param path  absolute file path for @logFile
   */
  private void putIfAbsent(LogFile logFile, String path) {
    synchronized (logFilesInfoLock) {
      LogFileAndPath logFileAndPath = new LogFileAndPath(logFile, path);
      int index = findPositionInStream(path, SingerUtils.getFileLastModifiedTime(path));
      if (index < logFilePaths.size()) {
        logFilePaths.add(index, logFileAndPath);
        for (int i = index; i < logFilePaths.size(); i++) {
          LogFileAndPath fileAndPath = logFilePaths.get(i);
          logFilePathsIndex.put(fileAndPath.getLogFile(), i);
        }
      } else {
        logFilePaths.add(logFileAndPath);
        logFilePathsIndex.put(logFile, logFilePaths.size() - 1);
      }
    }
  }

  /**
   * Add a log file to LogStream while keeping  the log files in sorted order
   * @param logFile the log file object that has inode info
   * @param path  the absolute file path
   */
  public void put(LogFile logFile, String path) {
    synchronized (logFilesInfoLock) {
      LOG.debug("update logfile path info: {} {}", logFile, path);
      if (!logFilePathsIndex.containsKey(logFile)) {
        putIfAbsent(logFile, path);
      } else {
        int index = logFilePathsIndex.get(logFile);
        logFilePaths.set(index, new LogFileAndPath(logFile, path));
      }
    }
  }

  /**
   * append the [LogFile, Path] info at the end of LogFilePaths list.
   */
  public void append(LogFile logFile, String path) {
    synchronized (logFilesInfoLock) {
      if (!logFilePathsIndex.containsKey(logFile)) {
        putIfAbsent(logFile, path);
      } else {
        int index = logFilePathsIndex.get(logFile);
        // the file is not the last one in the list. we need to update logFilePaths
        // and logFilePathsIndex
        logFilePaths.remove(index);
        for (int i = index; i < logFilePaths.size(); i++) {
          LogFileAndPath fileAndPath = logFilePaths.get(i);
          logFilePathsIndex.put(fileAndPath.getLogFile(), i);
        }
        logFilePaths.add(new LogFileAndPath(logFile, path));
        logFilePathsIndex.put(logFile, logFilePaths.size() - 1);
      }
    }
  }

  /**
   * We update the file path info when a file deletion event is received. As the file has
   * been removed at this time, we will not be able to get the inode info.
   * @param path
   */
  public void removeLogFilePathInfo(String path) {
    synchronized (logFilesInfoLock) {
      int index = -1;
      for (int i = 0; i < logFilePaths.size(); i++) {
        LogFileAndPath logFileAndPath = logFilePaths.get(i);
        if (logFileAndPath == null || logFileAndPath.getPath() == null) {
          LOG.error("Invalid logFilePaths at index : {}", i);
        } else if (path.equals(logFileAndPath.getPath())) {
          index = i;
          break;
        }
      }
      if (index >= 0) {
        LogFileAndPath fileAndPath = logFilePaths.get(index);
        logFilePathsIndex.remove(fileAndPath.getLogFile());
        logFilePaths.remove(index);
        for (int i = index; i < logFilePaths.size(); i++) {
          fileAndPath = logFilePaths.get(i);
          logFilePathsIndex.put(fileAndPath.getLogFile(), i);
        }
      }
    }
  }

  /**
   * @param logFile LogFile for which to get filesystem path.
   * @return path of the LogFile in filesystem. or null if @logFile is not in the log stream
   */
  public String getLogFilePath(LogFile logFile) {
    int retryNum = 0;
    synchronized (logFilesInfoLock) {
      while (!logFilePathsIndex.containsKey(logFile) && retryNum < MAX_RETRY) {
        LOG.error("LogStream {} does not contain key for {}", this, logFile);
        try {
          initialize();
          if (!logFilePathsIndex.containsKey(logFile)) {
            Thread.sleep(SingerSettings.getSingerConfig().getLogFileRotationTimeInMillis());
          }
        } catch (Exception e) {
          LOG.error("Exception in stream initialization", e);
        }
        retryNum++;
      }
      String retVal = null;
      if (retryNum < MAX_RETRY) {
        int index = logFilePathsIndex.get(logFile);
        if (index < 0 || index >= logFilePaths.size()) {
          LOG.error("Failed to find path for log file {}, index = {}", logFile, index);
        } else {
          retVal = logFilePaths.get(index).getPath();
        }
      }
      return retVal;
    }
  }

  /**
   * @param logFile LogFile to which the next LogFile wanted.
   * @return the next LogFile after given LogFile in the LogFile sequence in the LogStream. Return
   * null if the given LogFile is already the last LogFile in the LogStream.
   * @throws LogStreamException when the LogStream does not have the LogFile.
   */
  public LogFileAndPath getNext(LogFile logFile) throws LogStreamException {
    synchronized (logFilesInfoLock) {
      int index = logFilePathsIndex.get(logFile);
      if (index < 0) {
        LOG.error("Cannot find log file: {} in log stream: {}", logFile, logStreamName);
        throw new LogStreamException(
            "log file: " + logFile + " does not exist in log stream: " + getLogStreamDescriptor());
      } else if (index >= logFilePaths.size() - 1) {
        return null;
      } else {
        return logFilePaths.get(index + 1);
      }
    }
  }

  /**
   * Get the previous file in the log stream sequence
   * @param logFile the current logFile (with inode info)
   * @return the previous logFile in the sequence
   * @throws LogStreamException when the LogStream does not have the LogFile
   */
  public LogFileAndPath getPrev(LogFile logFile) throws LogStreamException {
    synchronized (logFilesInfoLock) {
      int index = logFilePathsIndex.get(logFile);
      if (index < 0) {
        LOG.error("Cannot find log file: {} in log stream: {}", logFile, logStreamName);
        throw new LogStreamException(String.format("log file: %s does not exist in log "
            + "stream: %s", logFile.toString(), getLogStreamDescriptor()));
      } else if (index == 0 || index >= logFilePaths.size()) {
        return null;
      } else {
        return logFilePaths.get(index - 1);
      }
    }
  }

  private boolean deleteOneFile(LogFileAndPath logFilePath, long now, int retentionInSeconds){
    boolean retval = false;
    File file = new File(logFilePath.getPath());
    long timeDeltaInMillis = now - file.lastModified();
    LOG.info("Checking " + logFilePath + ". Its delta mTime is " + timeDeltaInMillis);

    if (timeDeltaInMillis / 1000 > retentionInSeconds) {
      try {
        long inode = SingerUtils.getFileInode(logFilePath.getPath());
        if (inode == logFilePath.getLogFile().inode) {
          LOG.info("Deleting " + logFilePath + " as it's age > {} seconds", retentionInSeconds);
          if (!file.delete()) {
            LOG.error("Cannot delete {} since it does not exist.", logFilePath);
          } else {
            OpenTsdbMetricConverter.incr(SingerMetrics.LOGSTREAM_FILE_DELETION, 1,
                "log=" + logStreamName, "host=" + SingerUtils.getHostname());
            retval = true;
          }
        }
      } catch (Exception e) {
        LOG.error("Unable to remove file {}: ", logFilePath, e);
      }
    }
    return retval;
  }

  /**
   * Removes a given log file from the current log stream if
   * the file is older than the retention specified.
   * @param logFile the logFile to remove (with inode info)
   * @param retentionInSeconds the maximum age of the log file based on its mTime
   */
  public void removeOldFiles(LogFile logFile, int retentionInSeconds) {
    synchronized (logFilesInfoLock) {
      int logFileIndex = -1;
      try {
        logFileIndex = logFilePathsIndex.get(logFile);
      } catch (Exception e) {
        LOG.error("Failed to find {} in logFilePathsIndex", logFile);
      }
      if (logFileIndex < 0) {
        LOG.error("Logfile {} cannot be found, so it cannot be removed.", logFile);
        return;
      }

      // logFileIndex >= 0
      int curLogFileIndex = 0;
      long currentTime = System.currentTimeMillis();
      // the first index of logFiles is always the oldest log file in the current log stream
      while (curLogFileIndex < logFileIndex) {
        LogFileAndPath logFilePath = logFilePaths.get(curLogFileIndex);
        if (deleteOneFile(logFilePath, currentTime, retentionInSeconds)) {
          curLogFileIndex++;
        } else {
          break;
        }
      }

      // re-index log paths
      logFilePaths = new ArrayList<>(logFilePaths.subList(curLogFileIndex, logFilePaths.size()));
      logFilePathsIndex.clear();
      for (int i = 0; i < logFilePaths.size(); i++) {
        logFilePathsIndex.put(logFilePaths.get(i).getLogFile(), i);
      }
    }
  }

  /**
   * @return whether the LogStream contains any LogFile.
   */
  public boolean isEmpty() {
    synchronized (logFilesInfoLock) {
      return logFilePaths.isEmpty();
    }
  }

  /**
   * @param logFile
   * @return whether the LogStream has the given LogFile.
   */
  public boolean hasLogFile(LogFile logFile) {
    synchronized (logFilesInfoLock) {
      return logFilePathsIndex.containsKey(logFile);
    }
  }

  public List<String> getLogFilePaths() throws LogStreamException {
    synchronized (logFilesInfoLock) {
      return logFilePaths.stream().map(lfp -> lfp.getPath()).collect(Collectors.toList());
    }
  }

  @Override
  public boolean equals(Object object) {
    if (object == null) {
      return false;
    }
    if (!LogStream.class.isInstance(object)) {
      return false;
    }

    LogStream logStream = (LogStream) object;
    return Objects.equal(getLogStreamDescriptor(), logStream.getLogStreamDescriptor());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).addValue(getLogStreamDescriptor()).toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getLogStreamDescriptor());
  }

  /**
   * Check the consistency of data structures. This method is only used in tests.
   */
  public boolean checkConsistency() {
    synchronized (logFilesInfoLock) {
      for (int i = 0; i < logFilePaths.size() - 1; i++) {
        File file1 = new File(logFilePaths.get(i).getPath());
        File file2 = new File(logFilePaths.get(i + 1).getPath());
        if (!file1.exists()) {
          LOG.warn("Consistency check warning: {} does not exist", file1);
        } else if (!file2.exists()) {
          LOG.warn("Consistency check warning: {} does not exist", file2);
        } else {
          if (file1.lastModified() > file2.lastModified()) {
            LOG.error(" file1.lastModi < file2.lastModi {} < {}, {} < {}",
                file1, file2, file1.lastModified(), file2.lastModified());
            return false;
          } else if (file1.lastModified() == file2.lastModified()
              && file1.getName().length() == file2.getName().length()
              && file1.getName().compareTo(file2.getName()) < 0) {
            LOG.error("file.name > file2.name {} > {}", file1, file2);
            return false;
          }
        }
      }
    }
    return true;
  }
  
  public void clearAllLogPaths() {
      synchronized (logFilesInfoLock) {
          logFilePathsIndex.clear();
          logFilePaths.clear();
          LOG.warn("Cleared logstream:" + this.toString());
      }
  }

  /**
   * Log the stauts of this logstream. This method is only used for testing and debugging.
   */
  public void logStatus() {
    LOG.info("Begin log status: {}", this);
    for (int i = 0; i < logFilePaths.size(); i++) {
      LOG.info("{}:{}", i, logFilePaths.get(i));
    }
    LOG.info("End log status: {}", this);
  }

  public void setLastCompletedCycleTime(long lastCompleteCycleTime) {
    this.lastCompleteCycleTime = lastCompleteCycleTime;
  }
  
  public long getLastCompleteCycleTime() {
    return lastCompleteCycleTime;
  }
}
