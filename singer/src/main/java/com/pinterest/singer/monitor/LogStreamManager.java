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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.comparator.CompositeFileComparator;
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.apache.commons.io.comparator.NameFileComparator;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.common.SingerLog;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.common.errors.LogStreamException;
import com.pinterest.singer.common.errors.SingerLogException;
import com.pinterest.singer.kubernetes.KubeService;
import com.pinterest.singer.kubernetes.PodWatcher;
import com.pinterest.singer.thrift.LogFile;
import com.pinterest.singer.thrift.configuration.FileNameMatchMode;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.utils.SingerUtils;
import com.twitter.ostrich.stats.Stats;

/**
 * Singer thrift lib logs messages into log streams. LogStream is an abstract
 * concept in singer that captures log streams that are sequences of log files.
 *
 * This class captures LogStram related information, and provide api for
 * LogStream creation, deletion, [file path -> inode] mapping update, etc.
 */
public class LogStreamManager implements PodWatcher {

  private static final String POD_LOGNAME_SEPARATOR = "..";
  // use an empty string as non-kubernetes pod id for backward compatibility
  public static final String NON_KUBERNETES_POD_ID = "";
  private static final Logger LOG = LoggerFactory.getLogger(LogStreamManager.class);
  private static LogStreamManager instance;

  /**
   * This needs to be thread safe since pods are dynamically created and deleted using an async
   * process. Additionally this data structure is accessed by several threads including DefaultLogMonitor
   * , async delete task (LogStreamManager), RecursiveFSEventProcessor and MissingDirChecker.
   */
  private final SortedMap<String, Collection<LogStream>> dirStreams = new ConcurrentSkipListMap<>();

  /**
   * This data structure needs to be thread safe as LogStreamManager and
   * MissingDirChecker will access it.
   */
  private final Map<SingerLog, String> singerLogsWithoutDir = new ConcurrentHashMap<>();

  /**
   * This needs to be threadsafe since pods are dynamically created and new LogStreams
   * are registered via RecursiveFSEventProcessor which is a separate thread than FileSystemMonitor
   * and LogDirectoriesScanner
   */
  private Map<String, Set<SingerLog>> singerLogPaths = new ConcurrentHashMap<>();
  private String podLogDirectory = "";

  private FileSystemEventFetcher recursiveDirectoryWatcher;
  private Thread recursiveEventProcessorThread;
  private RecursiveFSEventProcessor recursiveEventProcessor;
  private MissingDirChecker missingDirChecker;

  @VisibleForTesting
  public MissingDirChecker getMissingDirChecker() {
    return missingDirChecker;
  }

  protected LogStreamManager() {
    missingDirChecker = new MissingDirChecker();
    if(SingerSettings.getSingerConfig()!=null &&
      SingerSettings.getSingerConfig().isKubernetesEnabled()) {
      KubeService.getInstance().addWatcher(this);
      podLogDirectory = SingerSettings.getSingerConfig().getKubeConfig().getPodLogDirectory();
      try {
        recursiveDirectoryWatcher = new FileSystemEventFetcher();
        recursiveDirectoryWatcher.start("RecursiveDirectoryWatcher");

        recursiveEventProcessor = new RecursiveFSEventProcessor(this);
        recursiveEventProcessorThread = new Thread(recursiveEventProcessor);
        recursiveEventProcessorThread.setDaemon(true);
        recursiveEventProcessorThread.start();
        recursiveEventProcessorThread.setName("RecursiveEventProcessor");
      } catch (IOException e) {
        LOG.error("Error initializing FS Event Fetcher for recursive directory watch", e);
        throw new RuntimeException(e);
      }
    }
  }

  public static LogStreamManager getInstance() {
    if (instance == null) {
      synchronized (LogStreamManager.class) {
        if (instance == null) {
          instance = new LogStreamManager();
        }
      }
    }
    return instance;
  }

  public static Collection<LogStream> getLogStreams() {
    Collection<Collection<LogStream>> streamCollections
        = LogStreamManager.getInstance().dirStreams.values();
    Stream<LogStream> streams = streamCollections.stream().flatMap(Collection::stream);
    return streams.collect(Collectors.toList());
  }

  public static Collection<LogStream> getLogStreams(Path path) {
    Collection<LogStream> result = null;
    if (LogStreamManager.getInstance().dirStreams.containsKey(path.normalize().toString())) {
      result = LogStreamManager.getInstance().dirStreams.get(path.normalize().toString());
    }
    return result;
  }

  /**
   * Finds SingerLog objects that could contain the a file, based on the file name and the log directory.
   * @param logDir The directory containing the log file
   * @param logFile The log file found in that directory
   * @return The SingerLog that could contain this log file, or null if it does not match any
   * logs in that directory.
   */
  public static List<SingerLog> getMatchedSingerLogs(Path logDir, File logFile) {
    return LogStreamManager.getInstance().getMatchedSingerLogsInternal(logDir, logFile);
  }

  private List<SingerLog> getMatchedSingerLogsInternal(Path parentDir, File logFile) {
    LOG.debug("Getting Singer Logs for " + logFile.toString() + " in " + parentDir);
    List<SingerLog> singerLogs = new ArrayList<>();
    String pathStr = parentDir.toString();
    if (singerLogPaths.containsKey(pathStr)) {
      for (SingerLog log : singerLogPaths.get(pathStr)) {
        String regex = log.getSingerLogConfig().getLogStreamRegex();
        LOG.debug("Checking..." + regex + " matching " + logFile.toString());
        if (new RegexFileFilter(regex).accept(logFile)) {
          singerLogs.add(log);
        }
      }
    }
    if (singerLogs.isEmpty()) {
      LOG.debug("Did not find any matched SingerLog for {}", logFile);
    }
    return singerLogs;
  }

  /**
   * Creates a new LogStream object from a file and this configuration.
   * @return The LogStream object that contains this file
   */
  public static LogStream createNewLogStream(SingerLog singerLog, String fileName) {
    return LogStreamManager.getInstance().createNewLogStreamInternal(singerLog, fileName);
  }

  private LogStream createNewLogStreamInternal(SingerLog singerLog, String fileName) {
    LogStream logStream = new LogStream(singerLog, fileName);
    Path logDir = new File(singerLog.getSingerLogConfig().getLogDir()).toPath();
    registerLogStreamInDirLogStreamMap(logDir, logStream);
    LOG.debug("Created and registered new logstream:" + logDir);
    return logStream;
  }


  public static void removeLogStream(LogStream logStream) {
     LogStreamManager.getInstance().removeLogStreamInternal(logStream);
  }

  private void removeLogStreamInternal(LogStream logStream) {
    Path logDir =  new File(logStream.getLogDir()).toPath();
    if (dirStreams.containsKey(logDir.normalize().toString())) {
      Collection<LogStream> streams = dirStreams.get(logDir.normalize().toString());
      streams.remove(logStream);
    }
  }

  /**
   * Initializes each SingerLog and adds all LogStreams to the FileSystemMonitor.
   *
   * @throws SingerLogException if there is a problem with one of the LogStreams
   */
  public static void initializeLogStreams() throws SingerLogException {
    LogStreamManager.getInstance().initializeLogStreamsInternal();
  }

  public static void initializeLogStreams(SingerLog singerLog) throws SingerLogException {
    // removed file system monitor initialization check
    LogStreamManager.getInstance().initializeLogStreamsInternal(NON_KUBERNETES_POD_ID, singerLog);
  }

  /**
   * Initialize logStreams given singerLog and podUid.  This method is called by MissingDirChecker
   * after it finds that a log directory is newly created.
   *
   * @param singerLog
   * @param podUid
   * @throws SingerLogException
   */
  public static void initializeLogStreams(SingerLog singerLog, String podUid) throws SingerLogException{
    LogStreamManager.getInstance().initializeLogStreamsInternal(podUid, singerLog);
  }

  private void initializeLogStreamsInternal() throws SingerLogException{
    Preconditions.checkNotNull(SingerSettings.getSingerConfig());
    LOG.info("Initialize log streams");
    List<SingerLogConfig> logConfigs = SingerSettings.getSingerConfig().getLogConfigs();

    if (logConfigs!=null) {
       for (SingerLogConfig singerLogConfig : logConfigs) {
          String logPathKey = singerLogConfig.getLogDir();
          SingerLog singerLog = new SingerLog(singerLogConfig);
          if (!singerLogPaths.containsKey(logPathKey)) {
            singerLogPaths.put(logPathKey, new HashSet<>());
          }
          singerLogPaths.get(logPathKey).add(singerLog);
          initializeLogStreamsInternal(NON_KUBERNETES_POD_ID, singerLog);
        }
      LOG.info("set singerLogsWithoutDir and start MissingDirChecker thread.");
      missingDirChecker.setSingerLogsWithoutDir(singerLogsWithoutDir);
      missingDirChecker.start();
      LOG.info("MissingDirChecker thread is running in the background.");
    } else {
        LOG.error("No log configs defined:" + SingerSettings.getSingerConfig().getLogConfigs());
    }
  }

  /**
   * Initializes LogStreams in this SingerLog and adds them to the FileSystemMonitor. Called
   * once, during the initialization of a LogMonitor and FileSystemMonitor, to set up the
   * FileSystemMonitor thread. The FileSystemMonitor should not have been started yet, since
   * if it has, events may be missed.
   * @throws SingerLogException if there is a problem reading from disk or the log stream is invalid
   */
  private void initializeLogStreamsInternal(String podUid, SingerLog singerLog) throws SingerLogException {
    try {
      SingerLogConfig singerLogConfig = singerLog.getSingerLogConfig();
      String logDir = singerLogConfig.getLogDir();
      Path logDirPath = SingerUtils.getPath(logDir);

      SingerSettings.getOrCreateFileSystemMonitor(podUid).registerPath(logDirPath);
      LOG.info("LogDir: {}", logDir);

      File dir = new File(logDir);
      if (dir.exists()) {
        String regexStr = singerLogConfig.getLogStreamRegex();
        LOG.info("Attempting to match files under {} with filter {}", logDirPath.toFile().getAbsolutePath(), regexStr);
        // @variable files contains files for a list of log streams. Each file represents one stream
        FileFilter fileFilter = new RegexFileFilter(regexStr);
        File[] files = dir.listFiles(fileFilter);

        LOG.info(files.length + " files matches the regex " + regexStr);
        if (singerLogConfig.getFilenameMatchMode() == FileNameMatchMode.EXACT) {
          for (File file : files) {
            LogStream stream = new LogStream(singerLog, FilenameUtils.getName(file.getPath()));
            this.registerLogStreamInDirLogStreamMap(logDirPath, stream);
            long inode = SingerUtils.getFileInode(file.getAbsolutePath());
            stream.append(new LogFile(inode), file.getPath());
          }
        } else if (singerLogConfig.getFilenameMatchMode() == FileNameMatchMode.PREFIX) {
          createPrefixBasedLogStreams(singerLog, singerLogConfig, dir, files, logDirPath);
        }
      } else if (NON_KUBERNETES_POD_ID.equals(podUid)) {
          singerLogsWithoutDir.putIfAbsent(singerLog, podUid);
      }
    } catch (NoSuchFileException e) {
      Stats.incr(SingerMetrics.NO_SUCH_FILE_EXCEPTION);
      LOG.warn("Missing LogStream file: " + e.getMessage());
    } catch (IOException e) {
      throw new SingerLogException("Bad log streams in this singer log", e);
    }
  }

  /**
   * Creates a LogStream object for a newly discovered file, if it belongs to one monitored
   * by Singer, and register the LogStream object in  @inodes map.
   *
   * @param singerLog The singer log configuration
   * @param fullAddedPath The full path of the file
   * @return a boolean indicating whether a LogStream could be created.
   * @throws IOException
   */
  public LogStream createLogStream(SingerLog singerLog, Path fullAddedPath) throws IOException {
    LogStream logStream = createNewLogStreamInternal(singerLog, fullAddedPath.toFile().getName());
    long inode = SingerUtils.getFileInode(fullAddedPath);
    logStream.append(new LogFile(inode), fullAddedPath.toString());
    return logStream;
  }

  /**
   * Creates LogStream objects for each log stream contained in files[] and adds them to
   * FileSystemMonitor.
   *
   * For instance, for a directory that have four files:
   *    api_request_log_7016.log
   *    api_request_log_7016.log.1454778266448
   *    api_request_log_7017.log
   *    api_request_log_7017.log.12321321321321
   *
   * We create a map as follows:
   *
   * LogStreamInodes Map (2) :
   *   LogStream{data.api_request_log:api_request_log_7016.log}:
   *      {api_request_log_7016.log.1454778266448->1313821,api_request_log_7016.log->1313826,}
   *  LogStream{data.api_request_log:api_request_log_7017.log}:
   *      {api_request_log_7017.log.12321321321321->1313819,api_request_log_7017.log->1313818,}
   *
   * @param files A list of files in a particular log directory that match a group of log streams
   * @param logDirPath The path to the directory containing the log files
   * @throws LogStreamException If there is an error creating the log stream
   * @throws IOException If there is an error reading from disk
   */
  @SuppressWarnings("unchecked")
  private void createPrefixBasedLogStreams(SingerLog singerLog,
      SingerLogConfig singerLogConfig, File logDir, File[] files, Path logDirPath) throws IOException {
    Map<String, LogStream> nameToLogStream = new HashMap<>();
    Map<String, ConcurrentHashMap<String, Long>> nameToFileInodes = new HashMap<>();

    for (File file : files) {
      LogStream stream = new LogStream(singerLog, file.getName());
      this.registerLogStreamInDirLogStreamMap(logDirPath, stream);
      nameToLogStream.put(file.getName(), stream);
      nameToFileInodes.put(file.getName(), new ConcurrentHashMap<>());
    }

    // get all files that matches the log stream regex prefix
    String regexStr = singerLogConfig.getLogStreamRegex() + ".*";
    FileFilter fileFilter = new RegexFileFilter(regexStr);
    List<File> logFiles = Arrays.asList(logDir.listFiles(fileFilter));

    // Sort the file first by last_modified timestamp and then by name in case two files have
    // the same mtime due to precision (mtime is up to seconds).
    @SuppressWarnings("rawtypes")
    Ordering ordering = Ordering.from(
        new CompositeFileComparator(
            LastModifiedFileComparator.LASTMODIFIED_COMPARATOR, NameFileComparator.NAME_REVERSE));
    logFiles = ordering.sortedCopy(logFiles);

    for (File file : logFiles) {
      String fileName = file.getName();
      for (String streamName : nameToLogStream.keySet()) {
        if (fileName.startsWith(streamName)) {
          long inode = SingerUtils.getFileInode(file.getAbsolutePath());
          LogStream logStream = nameToLogStream.get(streamName);
          nameToFileInodes.get(streamName).put(file.getName(), inode);
          logStream.append(new LogFile(inode), file.getAbsolutePath());
        }
      }
    }
  }

  /**
   * Adds a log stream to be associated with a path (the directory its files reside in)
   * @param logDir The directory holding the log files in the LogStream
   * @param logStream The LogStream to track
   */
  private void registerLogStreamInDirLogStreamMap(Path logDir, LogStream logStream) {
    if (!dirStreams.containsKey(logDir.normalize().toString())) {
      // Concurrent Set is needed to guard against Concurrent Modification Exception
      // can't use ConcurrentSkipListSet because that requires a comparable type
      dirStreams.put(logDir.normalize().toString(), Collections.newSetFromMap(new ConcurrentHashMap<>()));
    }
    dirStreams.get(logDir.normalize().toString()).add(logStream);
  }

  /**
   * Adds a LogStream to the FileSystemMonitor by watching its directory and reading the current
   * state of its inodes.
   * Currently only used for testing.
   * @param logStream The LogStream to watch
   */
  public static void addLogStream(LogStream logStream) {
    LogStreamManager.getInstance().addLogStreamInternal(logStream);
  }

  private void addLogStreamInternal(LogStream logStream) {
    Path logDir = SingerUtils.getPath(logStream.getLogDir());
    try {
      if (!dirStreams.containsKey(logDir.normalize().toString())) {
        dirStreams.put(logDir.normalize().toString(), Collections.newSetFromMap(new ConcurrentHashMap<>()));
      }
      dirStreams.get(logDir.normalize().toString()).add(logStream);
      LOG.info("Added LogStream " + logStream.getLogStreamDescriptor());
    } catch (Exception e) {
      Stats.incr(SingerMetrics.IO_EXCEPTION_METRIC_NAME);
      LOG.error("Error registering " + logDir.getFileName() + " from LogStream " +
          logStream.getLogStreamName(), e);
    }
  }

  /**
   * Gets the LogStreams that contain a particular file. There might be multiple logstreams that
   * match the same log file due to double publishing data to multiple destinations.
   * @param dir The directory containing the file
   * @param file The file that may belong to a log stream in that directory
   * @return A list of LogStreams that contains this file. It will be an empty list if there is no matching
   *         log streams.
   */
  public static List<LogStream> getLogStreamsFor(Path dir, Path file) {
    return LogStreamManager.getInstance().getLogStreamsForInternal(dir, file);
  }

  private List<LogStream> getLogStreamsForInternal(Path dir, Path file) {
    Collection<LogStream> logStreams = dirStreams.get(dir.normalize().toString());
    List<LogStream> result = new ArrayList<>();
    if (logStreams != null) {
      String fileName = file.toString();
      for (LogStream stream : logStreams) {
        if (stream.acceptFile(fileName)) {
          result.add(stream);
        }
      }
    }
    return result;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("DirStreams (" + dirStreams.size() + "):\n");
    for (Map.Entry<String, Collection<LogStream>> entry : dirStreams.entrySet()) {
      sb.append(entry.getKey()).append(" : ");
      for (LogStream stream : entry.getValue()) {
        sb.append(stream).append(", ");
      }
      sb.append("\n");
    }
    return sb.toString();
  }

  @VisibleForTesting
  public Map<String, Set<SingerLog>> getSingerLogPaths() {
    return singerLogPaths;
  }

  /**
   * Stop threads inside LogStreamManager 
   */
  public void stop() {
      if(recursiveDirectoryWatcher != null) {
          recursiveDirectoryWatcher.stop();
      }
      if(recursiveEventProcessorThread != null) {
          recursiveEventProcessorThread.interrupt();
      }
      if(missingDirChecker != null){
        missingDirChecker.stop();
      }
  }

  /**
   * Reset LogStream manager for testing purpose
   */
  public static void reset() {
      instance.stop();
      instance = null;
  }

  public FileSystemEventFetcher getRecursiveDirectoryWatcher() {
    return recursiveDirectoryWatcher;
  }

  @Override
  public void podCreated(String podUid) {
    // deferred pod stream initialization
    try {
        File podParent = new File(podLogDirectory + "/" + podUid + "/");
        LOG.info("Registering pod parent directory with recursive watch service:"+podParent.getAbsolutePath());
        recursiveDirectoryWatcher.registerPath(podParent.toPath());
        recursiveEventProcessor.evaluateAndRegisterLogStreamOrWatcher(podParent.toPath(), podUid);
    } catch (IOException e) {
        LOG.error("Failed to setup directory watch listener for new POD:" + podUid, e);
    }

  }

  @Override
  public void podDeleted(final String podUid) {
      int deletionCheckIntervalInSeconds = SingerSettings.getSingerConfig().getKubeConfig().getDeletionCheckIntervalInSeconds();

      SingerSettings.getBackgroundTaskExecutor().schedule(new Runnable() {
          @Override
          public void run() {
              LOG.info("Checking if POD:" + podUid + " is ready for deletion");

              // check if all logs for this pod are cleaned up
              String fromKey = new File(podLogDirectory + "/" + podUid).toPath().normalize().toString();
              String toKey = fromKey + "/" + Character.MAX_VALUE;

              // get all logstreams for this pod uid
              SortedMap<String, Collection<LogStream>> logStreamsForPodPath = dirStreams.subMap(fromKey, toKey);

              // iterate through the subMap
              long maxElapsedTime = checkAndCleanupLogStreams(podUid, logStreamsForPodPath,
                      SingerSettings.getSingerConfig().getKubeConfig().getDefaultDeletionTimeoutInSeconds());

              if(logStreamsForPodPath.size()==0) {
                // no more directory streams left, we can now cleanup singer and write a pod tombstone marker
                try {
                    SingerSettings.getOrCreateFileSystemMonitor(podUid).destroy();
                    SingerUtils.createEmptyDotFile(podLogDirectory + "/." + podUid);
                    LOG.info("POD(" + podUid + ") is ready to be cleaned; written dot file");
                    // update deletion count metrics
                    Stats.incr(SingerMetrics.ACTIVE_POD_DELETION_TASKS, -1);
                    Stats.incr(SingerMetrics.PODS_TOMBSTONE_MARKER);
                    // update the max time elapsed before the pod was deleted
                    Double timeElapsed = ((Double)Stats.getGauge(SingerMetrics.POD_DELETION_TIME_ELAPSED).get());
                    Stats.setGauge(SingerMetrics.POD_DELETION_TIME_ELAPSED, Math.max(maxElapsedTime, timeElapsed));
                } catch (Exception e) {
                    LOG.error("Exception while cleaning up during pod deletion for pod:"+podUid, e);
                }
              }else {
                // reschedule itself again for review in a few seconds
                SingerSettings.getBackgroundTaskExecutor().schedule(this,
                          deletionCheckIntervalInSeconds, TimeUnit.SECONDS);
                  LOG.debug("Rescheduling cleanup check for pod(" + podUid + ")");
              }
          }

      }, deletionCheckIntervalInSeconds, TimeUnit.SECONDS);
      LOG.info("Scheduled task to check if pod directory is deletable for pod(" + podUid + ")");
      Stats.incr(SingerMetrics.ACTIVE_POD_DELETION_TASKS);
  }

  /**
   * Check if the supplied logstreams can be removed because there have been no new events for the supplied 
   * deletion timeout.
   *
   * Context: When a pod is stopped there still may be un-flushed data i.e. data that has not yet been
   * published to Kafka by Singer. This method is ensure that we cleanup any LogStreams that have been safely
   * written to Kafka and then timeout the ones that in pending state. We keep calling this method until
   * all LogStreams for a given pod have been deleted.
   *
   * @param podUid
   * @param subMap
   * @param deletionTimeout
   * @return
   */
  private long checkAndCleanupLogStreams(final String podUid, SortedMap<String, Collection<LogStream>> subMap,
          int deletionTimeout) {
      long maxDeltaTime = 0;
      for (Iterator<Entry<String, Collection<LogStream>>> dirStreamEntryIterator = subMap.entrySet().iterator(); dirStreamEntryIterator.hasNext();) {
          Entry<String, Collection<LogStream>> entry = dirStreamEntryIterator.next();
          for (Iterator<LogStream> logStreamIterator = entry.getValue().iterator(); logStreamIterator.hasNext();) {
              LogStream logStream = logStreamIterator.next();
              // if we have exceeded the timeout then remove this logstream
              // completed cycle time is updated by DefaultLogMonitor
              long lastCompleteCycleTime = logStream.getLastCompleteCycleTime();
              long latestPivotTimestamp = Math.max(logStream.getLatestProcessedMessageTime(), lastCompleteCycleTime);
              long latency = Math.max(logStream.getLastStreamModificationTime() - latestPivotTimestamp, 0);
              long deltaTimeInSeconds = (System.currentTimeMillis() - logStream.getLastStreamModificationTime())/1000;
              if(latency == 0 && deltaTimeInSeconds > deletionTimeout) {
                  LOG.debug(" Podstream("+logStream.getLogStreamName()+") timed out for pod("+podUid+") with path("
                           +logStream.getLogDir()+"@"+logStream.getFileNamePrefix()+")");
                  // Clearing all logpaths inside this logstream will make DefaultLogMonitor stop
                  // corresponding log processors in next iteration (log message: "Start monitoring cycle")
                  logStream.clearAllLogPaths();
                  LOG.debug("Cleared all log paths in logstream(" + logStream + ")");
                  logStreamIterator.remove();
              }
              maxDeltaTime = Math.max(deltaTimeInSeconds, maxDeltaTime);
          }
          // if there are no more log streams left then remove this path
          if(entry.getValue().size()==0) {
              dirStreamEntryIterator.remove();
              LOG.info("Removing Log directory (" + entry.getKey() + ") entry since there are no more log streams remaining");
          }
          LOG.info("For POD(" + podUid + ") Path logstream stream remaining:" + entry.getValue().size());
      }
      return maxDeltaTime;
  }

  /**
   * Initialize LogStreams for the supplied SingerLogConfigurations
   *
   * @param podUid
   * @param configs
   * @throws SingerLogException
   */
  public void initializeLogStreamForPod(String podUid, Collection<SingerLogConfig> configs) throws SingerLogException {
      // initialize logstreams for the supplied configs only
      for(SingerLogConfig singerLogConfig : configs) {
          String logDir = singerLogConfig.getLogDir();

          // treat kubernetes special
          String logPathKey =
              new File(podLogDirectory + "/" + podUid + "/" + logDir).getAbsolutePath();
          // note this key is actually an absolute path; if the path key above is not a real path, processing will not start for it
          LOG.info("For POD:" + podUid + " configuring logpath:" + logDir + " with key:" + logPathKey);

          SingerLog singerLog = null;

          // clone the log config and override the log directory from relative to absolute path
          SingerLogConfig clone = singerLogConfig.deepCopy();
          clone.setLogDir(logPathKey);
          // logstreams from two different pods were getting dropped
          // since the DefaultLogMonitor de-duplicates this using hashcode
          // which is dependent on LogStream name
          clone.setName(podUid + POD_LOGNAME_SEPARATOR + clone.getName());
          singerLog = new SingerLog(clone, podUid);

          if (!singerLogPaths.containsKey(logPathKey)) {
            singerLogPaths.put(logPathKey, new HashSet<>());
          }

          boolean added = singerLogPaths.get(logPathKey).add(singerLog);
          // we check to avoid duplicate start
          if(added) {
              LOG.info("New singerlog " + singerLog.getSingerLogConfig().getLogDir() + " for pod:" + podUid);
              initializeLogStreamsInternal(podUid, singerLog);
          }
      }
  }

  /**
   * Exposed for testing only
   * @return
   */
  public SortedMap<String, Collection<LogStream>> getDirStreams() {
    return dirStreams;
  }
}
