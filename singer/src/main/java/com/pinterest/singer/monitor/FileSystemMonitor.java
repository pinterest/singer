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
import com.pinterest.singer.common.SingerConfigDef;
import com.pinterest.singer.common.SingerLog;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.thrift.LogFile;
import com.pinterest.singer.thrift.LogFileAndPath;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.utils.SingerUtils;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.ostrich.stats.Stats;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.Collection;

/**
 * FileSystemMonitor keeps track of [file path -> inode] mapping for each log stream.
 *
 * Internally it maintains a few data structures;
 *    inodes     : logstream -> [path -> inode]
 *    dirStreams : directory -> log streams in the directory
 *    keys       : watch key -> path
 *    paths      : monitored path
 */
public class FileSystemMonitor implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemMonitor.class);

  private FileSystemEventFetcher fileSystemEventFetcher;
  private boolean cancelled = true;
  private Thread thread;
  private String name;

  public FileSystemMonitor(String name) throws IOException {
    this.name = name;
    initialize();
  }

  public FileSystemMonitor(SingerConfig singerConfig, String name) throws IOException {
    this.name = name;
    initialize();
  }

  /**
   * Constructs a FileSystemMonitor to keep track of log file names and inode numbers
   * @param streamsToWatch LogStreams to keep track of the contents
   * @throws IOException If the watch service can not be initialized
   */
  public FileSystemMonitor(Collection<LogStream> streamsToWatch, String name) throws IOException {
    this.name = name;
    initialize();
    for (LogStream stream : streamsToWatch) {
      Path logDir = SingerUtils.getPath(stream.getLogDir());
      fileSystemEventFetcher.registerPath(logDir);
      LogStreamManager.addLogStream(stream);
    }
  }

  /**
   *  Start the file system monitor thread if it has not been started yet.
   *
   */
  public void start() {
    // we don't want to start another thread for the same instance of FileSystemEventFetcher
    if(thread == null) {
        thread = new Thread(this);
        thread.setName("FileSystemMonitor-" + name);
        thread.start();
    }
  }

  /**
   * This method is idempotent
   */
  public void stop() {
    cancelled = true;
    if(fileSystemEventFetcher != null) {
        fileSystemEventFetcher.stop();
    }
    if (thread != null && thread.isAlive()) {
        thread.interrupt();
    }
  }

  public void join() throws InterruptedException {
    thread.join();
  }

  private void initialize() throws IOException {
    fileSystemEventFetcher = new FileSystemEventFetcher();
    fileSystemEventFetcher.start(name);
  }

  public void registerPath(Path logDir) throws IOException {
    fileSystemEventFetcher.registerPath(logDir);
  }

  private void handleEntryCreateEvent(Path parentDir,Path addedFile) throws IOException {
    if (addedFile.toString().startsWith(".")) {
      // ignore the watermark files
      LOG.debug("Found a watermark file {}", addedFile);
      return;
    }
    Path fullAddedPath = parentDir.resolve(addedFile);
    File theFile = fullAddedPath.toFile();
    if (!theFile.exists()) {
      LOG.error("Log file {} does not exist", fullAddedPath);
      Stats.incr(SingerMetrics.MISSING_LOG_FILES);
      return;
    }

    long inode = SingerUtils.getFileInode(fullAddedPath);
    List<LogStream> existingLogStreams = LogStreamManager.getLogStreamsFor(parentDir, fullAddedPath);

    // if we already find a log stream, update the inodes mapping.
    if (!existingLogStreams.isEmpty()) {
      LOG.info("created file " + addedFile.toString());
      // update stream info
      for (LogStream stream : existingLogStreams) {
        stream.put(new LogFile(inode), fullAddedPath.toString());
      }
    }

    // from here, create new log streams for the matched log stream configurations
    // TODO: to verify if this logMonitor check is redundant
    if (SingerSettings.getLogMonitor() == null) {
      LOG.error("SingerSettings.logMonitor is null");
      return;
    }

    List<SingerLog> singerLogs = LogStreamManager.getMatchedSingerLogs(parentDir, addedFile.toFile());
    if (singerLogs.isEmpty()) {
      LOG.info("Do not get matched singer log for {}", fullAddedPath);
    } else {
      // Match the singer logstream configurations that haven't been matched earlier
      Set<String> matchedSingerLogNames = new HashSet<>();
      for (LogStream logStream : existingLogStreams) {
        matchedSingerLogNames.add(logStream.getSingerLog().getLogName());
      }
      for (SingerLog singerLog : singerLogs) {
        if (!matchedSingerLogNames.contains(singerLog.getLogName())) {
          LogStream logStream = LogStreamManager.getInstance().createLogStream(singerLog, fullAddedPath);
          if (logStream == null) {
            LOG.warn("Failed to create a log stream for {}", fullAddedPath);
          }
        }
      }
    }
  }

  /**
   * When a file is renamed, the file system can generate two events: first is a deletion,
   * and then a creation. As the latest file is often the file that is being processing
   * by the stream processor, removing it from the LogStreamManager may make LogStreamProcessor
   * think that it loads an invalid watermark file, as the LogFile that is recorded in the
   * watermark file cannot be found in the log stream.
   *
   * INFO com.pinterest.singer.monitor.FileSystemMonitor - deleted singer_test_event
   * INFO com.pinterest.singer.monitor.FileSystemMonitor - added singer_test_event.2016-07-23-03.3
   * INFO com.pinterest.singer.monitor.FileSystemMonitor - added singer_test_event
   *
   */
  private void handleEntryDeleteEvent(Path parentDir,  Path deletedFile) {
    LOG.info("deleting " + deletedFile.toString());
    List<LogStream> logStreams = LogStreamManager.getLogStreamsFor(parentDir, parentDir.resolve(deletedFile));
    for (LogStream stream: logStreams) {
      LogFileAndPath latestLogFileAndPath = stream.getLatestLogFileAndPath();
      if (latestLogFileAndPath != null) {
        String latestPath = latestLogFileAndPath.getPath();
        if (latestPath == null) {
          LOG.error("Invalid path in logFilePath for log stream {}", stream);
        } else if (!latestPath.equals(deletedFile.toAbsolutePath().toString())) {
          Path fullPath = parentDir.resolve(deletedFile);
          stream.removeLogFilePathInfo(fullPath.toString());
          LOG.info("Removing {} from stream {}", fullPath, stream);
        }
      }
    }
  }

  /**
   * Handle the modified event. We can ignore the changes in watermark files that
   * starts with '.' character.
   * @throws IOException
   */
  private void handleEntryModifyEvent(Path logDir , Path modified ) throws IOException {
    Path fullPath = logDir.resolve(modified);
    long inode;
    try {
      inode = SingerUtils.getFileInode(fullPath);
    } catch(NoSuchFileException e) {
      LOG.warn("Failed to get inode info for " + fullPath, e);
      return;
    }

    List<LogStream> logStreams = LogStreamManager.getLogStreamsFor(logDir, fullPath);
    for (LogStream stream : logStreams) {
      if (stream != null) {
        stream.append(new LogFile(inode), fullPath.toString());
      } else if (!modified.toString().startsWith(".")) {
        LOG.debug("Found a file {} that is not in any log stream", modified);
      }
    }
  }

  /**
   * Checks for new events from the WatchService and adds or removes map entries based on the
   * events.
   */
  @VisibleForTesting
  public void processFileSystemEvents() throws InterruptedException {
    // Blocks until keys are present
    FileSystemEvent fileSystemEvent = fileSystemEventFetcher.getEvent();
    WatchEvent.Kind<?> kind = fileSystemEvent.event().kind();
    LOG.debug("Checking file system events: {}", kind);
    try {
      if (kind.equals(StandardWatchEventKinds.OVERFLOW)) {
        LOG.warn("Received overflow watch event from filesystem: Events may have been lost");
        Stats.incr(SingerMetrics.FS_EVENTS_OVERFLOW);
        LogDirectoriesScanner scanner =
            new LogDirectoriesScanner(fileSystemEventFetcher.getMonitoredPaths());
        scanner.start();
      } else {
        WatchEvent<?> event = fileSystemEvent.event();
        Path dirPath = fileSystemEvent.logDir();
        Path filePath = (Path) event.context();
        if (filePath.toString().startsWith(".")) {
          // ignore the watermark files
          LOG.debug("Ignore event for watermark file {}", filePath);
          return;
        }
        if (kind.equals(StandardWatchEventKinds.ENTRY_MODIFY)) {
          handleEntryModifyEvent(dirPath, filePath);
        } else if (kind.equals(StandardWatchEventKinds.ENTRY_CREATE)) {
          handleEntryCreateEvent(dirPath, filePath);
        } else if (kind.equals(StandardWatchEventKinds.ENTRY_DELETE)) {
          handleEntryDeleteEvent(dirPath, filePath);
        } else {
          LOG.error("Unexpected file system event {}:{}", kind, event.context());
        }
      }
    } catch (IOException e) {
      LOG.error("Encountered IOException : ", e);
      Stats.incr(SingerMetrics.IO_EXCEPTION_METRIC_NAME);
    }
  }

  public void updateAllDirectories() {
    LogDirectoriesScanner scanner =
        new LogDirectoriesScanner(fileSystemEventFetcher.getMonitoredPaths());
    scanner.start();
    try {
      scanner.join();
    } catch (InterruptedException e) {
      LOG.error("Interrupted while scaning the directories", e);
    }
  }
  /**
   * @return a boolean indicating whether the FileSystemMonitor has been started
   */
  public boolean isRunning() {
    return !cancelled;
  }

  @Override
  public void run() {
    cancelled = false;
    try {
      while (!cancelled) {
        try {
          processFileSystemEvents();
        } catch (InterruptedException e) {
          LOG.error("Filesystem Monitor thread was interrupted. Exiting gracefully.");
          Thread.currentThread().interrupt();
          break;
        } catch (Exception e) {
          LOG.error("Filesystem Monitor thread had an exception. "
                  + "Shutting down singer, don't panic singer will restart", e);
          System.exit(SingerConfigDef.SINGER_EXIT_FSM_EXCEPTION);
        }
      }
    } finally {
      destroy();
    }
  }

  /**
   * Closes the WatchService and ceases checking for additional events.
   */
  public void destroy() {
    cancelled = true;
    if (thread != null) {
      thread.interrupt();
    }
    if (fileSystemEventFetcher != null) {
      fileSystemEventFetcher.stop();
    }
  }
}