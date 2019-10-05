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

import com.pinterest.singer.common.LogMonitor;
import com.pinterest.singer.common.errors.LogMonitorException;
import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.LogStreamProcessor;
import com.pinterest.singer.common.LogStreamReader;
import com.pinterest.singer.common.errors.LogStreamReaderException;
import com.pinterest.singer.common.LogStreamWriter;
import com.pinterest.singer.common.errors.LogStreamWriterException;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.processor.DefaultLogStreamProcessor;
import com.pinterest.singer.reader.DefaultLogStreamReader;
import com.pinterest.singer.reader.TextLogFileReaderFactory;
import com.pinterest.singer.reader.ThriftLogFileReaderFactory;
import com.pinterest.singer.thrift.configuration.DummyWriteConfig;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.thrift.configuration.KafkaWriterConfig;
import com.pinterest.singer.thrift.configuration.LogStreamProcessorConfig;
import com.pinterest.singer.thrift.configuration.LogStreamReaderConfig;
import com.pinterest.singer.thrift.configuration.LogStreamWriterConfig;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.thrift.configuration.SingerRestartConfig;
import com.pinterest.singer.thrift.configuration.TextReaderConfig;
import com.pinterest.singer.thrift.configuration.ThriftReaderConfig;
import com.pinterest.singer.utils.SingerUtils;
import com.pinterest.singer.writer.DummyLogStreamWriter;
import com.pinterest.singer.writer.KafkaWriter;
import com.pinterest.singer.writer.pulsar.PulsarWriter;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.ostrich.stats.Stats;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * The default implementation of LogMonitor.
 * <p>
 * This default implementation of LogMonitor monitors for LogStreams in all configured SingerLogs.
 * It periodically wakes up and discovers all LogStreams in one SingerLog. If any LogStream is not
 * processed, it will start a DefaultLogStreamProcessor to process the LogStream.
 * <p>
 * This class is not thread-safe. monitorLogs() method does all the monitoring tasks and should
 * only be called from one thread at any time. The start() and stop() methods can be called in other
 * threads to start and stop the monitor.
 * <p>
 * TODO(wangxd): close processors for LogStreams in those logs that are no longer monitored.
 */
public class DefaultLogMonitor implements LogMonitor, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultLogMonitor.class);
  public static final String HOSTNAME = SingerUtils.getHostname();

  protected static LogMonitor INSTANCE;

  // If a stream hasn't been processed for this long time, it is considered stuck. Currently 1 min.
  private static final long MINIMUM_STUCK_STREAM_AGE_SECS = 60 * 10;

  // Monitoring interval in seconds.
  private final int monitorIntervalInSecs;

  // Map from LogStreams to their processors.
  private final Map<LogStream, LogStreamProcessor> processedLogStreams;

  // Whether this monitor is stopped. This should be accessed under the lock on "isStopped".
  private Boolean isStopped;

  // Handle to the next monitor run. This should be accessed under the lock on "isStopped".
  private ScheduledFuture<?> scheduledFuture;
  
  /**
   * The executor service for executing logMonitor thread. This needs to be independent of the
   * log processor. Otherwise, under heavy workload, logMonitor may not get cycle to run.
   */
  private ScheduledExecutorService logMonitorExecutor;

  private boolean dailyRestart = false;

  private long restartTimeInMillis = Long.MAX_VALUE;

  /**
   * Constructor.
   *
   * @param monitorIntervalInSecs monitor interval in seconds.
   * @param singerConfig          the SingerConfig.
   */
  protected DefaultLogMonitor(int monitorIntervalInSecs,
                              SingerConfig singerConfig)
      throws ConfigurationException {
    Preconditions.checkArgument(monitorIntervalInSecs > 0);
    this.monitorIntervalInSecs = monitorIntervalInSecs;
    this.processedLogStreams = Maps.newHashMap();
    this.isStopped = true;
    this.scheduledFuture = null;
    this.logMonitorExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("LogMonitor").build());
    if (singerConfig.isSetSingerRestartConfig() && singerConfig.singerRestartConfig.restartDaily) {
      dailyRestart = true;
      setDailyRestartTime(singerConfig.singerRestartConfig);
    }
  }

  /**
   * The configuration sets the daily restart time range. This method randomly picks up a time
   * during that range within the next 24 hours. By using randomly selected time in a range,
   * we can avoid singer on all hosts restart at the same time.
   *
   * @param restartConfig  the restart configuration
   * @throws ParseException
   */
  private void setDailyRestartTime(SingerRestartConfig restartConfig)
      throws ConfigurationException {
    Calendar c = new GregorianCalendar();
    c.set(Calendar.HOUR_OF_DAY, 0);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.SECOND, 0);
    Date date = c.getTime();
    Date startTime = SingerUtils.convertToDate(restartConfig.dailyRestartUtcTimeRangeBegin);
    Date endTime = SingerUtils.convertToDate(restartConfig.dailyRestartUtcTimeRangeEnd);
    Random rand = new Random(SingerUtils.getHostname().hashCode());
    long randomMillis = rand.nextInt((int) endTime.getTime() - (int) startTime.getTime() + 1)
        + startTime.getTime();
    restartTimeInMillis = date.getTime() + randomMillis;
    if (restartTimeInMillis < System.currentTimeMillis()) {
      restartTimeInMillis += 86400 * 1000;
    }
  }

  public static LogMonitor getInstance(int monitorIntervalInSecs, 
                                       SingerConfig singerConfig)
      throws ConfigurationException {
    if (INSTANCE == null) {
      synchronized (DefaultLogMonitor.class) {
        if (INSTANCE == null) {
          INSTANCE = new DefaultLogMonitor(monitorIntervalInSecs, singerConfig);
        }
      }
    }
    return INSTANCE;
  }

  /**
   * Monitor all SingerLogs configured.
   * <p>
   * This method is NOT thread-safe. It should be called from one thread at any time.
   *
   * @throws LogMonitorException when fail to monitor one of the SingerLogs.
   */
  public void monitorLogs() throws LogMonitorException {
    LOG.info("Start monitoring cycle.");
    long monitorCycleStartTime = System.currentTimeMillis();
    Collection<LogStream> logStreams = LogStreamManager.getLogStreams();
    for (LogStream logStream : logStreams) {
      String logStreamName = logStream.getLogStreamName();
      boolean success = false;
      try {
        LogStreamProcessor logStreamProcessor = processedLogStreams.get(logStream);
        if (logStreamProcessor == null) {
          LogStreamProcessor processor = createLogStreamProcessor(
              logStream.getSingerLog().getSingerLogConfig(), logStream);
          processor.start();
          processedLogStreams.put(logStream, processor);
          LOG.info("Start processing log stream: {} in log: {}", logStream, logStreamName);
        } else {
          // refresh the latest modification time.
          LOG.info("Log stream: {} in log: {} is being processed.", logStream, logStreamName);
          // set last completed latency in LogStream object so we don't have to look-up 
          // processor for logstream
          logStream.setLastCompletedCycleTime(logStreamProcessor.getLastCompleteCycleTime());
        }
        success = true;
      } catch (Exception e) {
        LOG.error("Exception in monitorLogs for " + logStream, e);
        Stats.incr("singer.monitor.unexpected_exception");
      }
      if (!success) {
        Stats.incr("singer.monitor.processor_creation_failure");
        LOG.error("Failed to create log monitor for {}", logStream);
      }
    }
    Stats.addMetric("singer.monitor.monitor_cycle_time",
        (int) (System.currentTimeMillis() - monitorCycleStartTime));
    Stats.setGauge(SingerMetrics.NUM_LOGSTREAMS, processedLogStreams.size());
  }

  /**
   * Monitor the log streams, and start the log stream processor for new log streams.
   *
   * @throws Exception when fail to monitor the SingerLog.
   */
  private LogStreamProcessor createLogStreamProcessor(SingerLogConfig singerLogConfig,
                                                      LogStream logStream)
      throws ConfigurationException, LogStreamReaderException, LogStreamWriterException {
    LogStreamReader reader =
        createLogStreamReader(logStream, singerLogConfig.getLogStreamReaderConfig());
    LogStreamWriter writer =
        createLogStreamWriter(logStream, singerLogConfig.getLogStreamWriterConfig());

    LogStreamProcessorConfig processorConfig = singerLogConfig.getLogStreamProcessorConfig();
    int batchSize = processorConfig.getBatchSize();
    batchSize = writer.isAuditingEnabled() ? batchSize - 1 : batchSize;

    return new DefaultLogStreamProcessor(
        logStream,
        singerLogConfig.getLogDecider(),
        reader,
        writer,
        batchSize,
        processorConfig.getProcessingIntervalInMillisecondsMin(),
        processorConfig.getProcessingIntervalInMillisecondsMax(),
        processorConfig.getProcessingTimeSliceInMilliseconds(),
        singerLogConfig.getLogRetentionInSeconds());
  }

  /**
   * Create a LogStreamReader for the LogStream based on LogStreamReaderConfig.
   *
   * @param logStream    LogStream to read from.
   * @param readerConfig reader config.
   * @return reader for the LogStream.
   * @throws Exception when fail to create the reader for the LogStream.
   */
  protected LogStreamReader createLogStreamReader(LogStream logStream,
                                                LogStreamReaderConfig readerConfig)
      throws LogStreamReaderException {
    switch (readerConfig.getType()) {
      case THRIFT:
        ThriftReaderConfig thriftReaderConfig = readerConfig.getThriftReaderConfig();
        return new DefaultLogStreamReader(logStream,
            new ThriftLogFileReaderFactory(thriftReaderConfig));

      case TEXT:
        TextReaderConfig textReaderConfig = readerConfig.getTextReaderConfig();
        return new DefaultLogStreamReader(logStream,
            new TextLogFileReaderFactory(textReaderConfig));

      default:
        throw new LogStreamReaderException("Unsupported log reader type");
    }
  }

  /**
   * Create a LogStreamWriter for the LogStream based on LogStreamWriterConfig.
   *
   * @param logStream    LogStream from which writer gets and writes LogMessages.
   * @param writerConfig writer config.
   * @return writer for the LogStream.
   * @throws Exception when fail to create a writer for the LogStream.
   */
  protected LogStreamWriter createLogStreamWriter(LogStream logStream,
                                                LogStreamWriterConfig writerConfig)
      throws ConfigurationException, LogStreamWriterException {
    switch (writerConfig.getType()) {
      case KAFKA08:
      case KAFKA:
        return createKafkaWriter(logStream, writerConfig.getKafkaWriterConfig());
      case DUMMY:
        return createDummyWriter(logStream, writerConfig.getDummyWriteConfig());
      case PULSAR:
        return new PulsarWriter().init(logStream, writerConfig.getPulsarWriterConfig());
      default:
        throw new LogStreamWriterException("Unsupported log writer type.");
    }
  }

  /**
   * Create Kafka writer.
   *
   * @param logStream           LogStream that writer read from.
   * @param kafkaWriterConfig Kafka writer config.
   * @return Kafka writer for this LogStream.
   */
  protected LogStreamWriter createKafkaWriter(LogStream logStream,
                                              KafkaWriterConfig kafkaWriterConfig)
      throws ConfigurationException {
    KafkaProducerConfig producerConfig = kafkaWriterConfig.getProducerConfig();

    String topic = extractTopicNameFromLogStreamName(
        logStream.getLogStreamName(),
        logStream.getSingerLog().getSingerLogConfig().getLogStreamRegex(),
        kafkaWriterConfig.getTopic());

    boolean auditingEnabled = kafkaWriterConfig.isAuditingEnabled();
    String auditTopic = null;
    if (auditingEnabled) {
      auditTopic = extractTopicNameFromLogStreamName(
          logStream.getLogStreamName(),
          logStream.getSingerLog().getSingerLogConfig().getLogStreamRegex(),
          kafkaWriterConfig.getAuditTopic());
    }

    int writeTimeoutInSeconds = kafkaWriterConfig.getWriteTimeoutInSeconds();
    String partitionerClass = producerConfig.getPartitionerClass();
    boolean enableHeadersInjector = logStream.getSingerLog().getSingerLogConfig().isEnableHeadersInjector();

    try {
      KafkaWriter kafkaWriter =
          new KafkaWriter(logStream, producerConfig, topic, kafkaWriterConfig.isSkipNoLeaderPartitions(),
              auditingEnabled, auditTopic, partitionerClass, writeTimeoutInSeconds, enableHeadersInjector);
      LOG.info("Created kafka writer : " + kafkaWriterConfig.toString());
      return kafkaWriter;
    } catch (Exception e) {
      throw new ConfigurationException(e);
    }
  }

  protected LogStreamWriter createDummyWriter(
      LogStream logStream, DummyWriteConfig dummyWriteConfig) throws ConfigurationException {
    String topic = extractTopicNameFromLogStreamName(
        logStream.getLogStreamName(),
        logStream.getSingerLog().getSingerLogConfig().getLogStreamRegex(),
        dummyWriteConfig.getTopic());
    return new DummyLogStreamWriter(logStream, topic);
  }

  /**
   * Start to periodically monitor LogStreams in all configured logs.
   * <p>
   * This method is thread safe.
   */
  @Override
  public void start() {
    synchronized (isStopped) {
      if (isStopped) {
        scheduledFuture = logMonitorExecutor.scheduleAtFixedRate(
            this, 0, monitorIntervalInSecs, TimeUnit.SECONDS);
        isStopped = false;
        LOG.info("Start log monitor which monitor logs every {} seconds.", monitorIntervalInSecs);
      } else {
        LOG.warn("LogMonitor already started when asked to start.");
      }
    }
  }

  /**
   * Stop monitoring and processing of LogStreams in all configured logs.
   * <p>
   * This method is thread-safe.
   */
  public void stop() {
    synchronized (isStopped) {
      if (!isStopped) {
        // Stop processing of current LogStreams.
        stopMonitoredLogs();

        Preconditions.checkNotNull(scheduledFuture, "LogMonitor is not running");
        // Do not interrupt since the log monitor is running now.
        scheduledFuture.cancel(false);

        // Wait until last run is done.
        try {
          scheduledFuture.get();
        } catch (InterruptedException e) {
          LOG.error("Interrupted while waiting", e);
        } catch (ExecutionException e) {
          // Ignore any exception from the scheduled run.
          LOG.error("Caught exception from the last monitoring cycle", e);
        } catch (CancellationException e) {
          // this is an expected exception
        } catch (Exception e) {
          LOG.error("Caught unexpected exception", e);
          Stats.incr("singer.monitor.unexpected_exception");
        }
        isStopped = true;
        
        // stop the thread pool
        logMonitorExecutor.shutdown();
        
        LOG.info("Stopped log monitor.");
      } else {
        LOG.warn("LogMonitor already stopped when asked to stop.");
      }
    }
  }

  /**
   * Check the daily singer restart setting, and restart singer
   * if the conditions are satisfied. Singer randomly selects a minute in the
   * restarting timerange, and restart singer daily at that minute (in UTC).
   */
  private void dailySingerRestartCheck() {
    long current = System.currentTimeMillis();
    if (restartTimeInMillis < current) {
      LOG.warn("Singer restarts on {}", new Date(current));
      SingerUtils.exit("Daily singer restart", 2);
    }
  }


  @Override
  public void run() {
    try {
      // process all configured log types.
      monitorLogs();
      // Cleanup log streams which are empty
      cleanUpLogs();
      // Report stats.
      reportStats();
    } catch (LogMonitorException t) {
      LOG.error("Caught exception when monitor log streams", t);
      Stats.incr("singer.monitor.exception");
    } catch (Exception e) {
      LOG.error("Caught unexpected exception", e);
      Stats.incr("singer.monitor.unexpected_exception");
    } catch (Throwable t) {
      LOG.error("Caught unexpected throwable. Exiting!", t);
      System.exit(1);
    }
    // restart Singer if necessary
    if (dailyRestart) {
      dailySingerRestartCheck();
    }
  }

  /**
   * We compute the following stats in this method. Ostrich can help to compute average, p50,
   * p95, p99, p999, etc.
   *    singer.processor.latency.* (average, p50, p95, p99, p999)
   *    singer.processor.max_unfiltered_processing_latency
   *    singer.processor.max_known_processing_latency
   *    singer.processor.stuck_processors
   *
   * The processing latency of each log stream is computed as
   *        LastLogStreamFileModificationTime - LastProcessedMessageTimestamp
   *
   * That is the upper bound of time span that Singer does not process any event from this log
   * stream.
   * The LastProcessedMessageTimestamp is  event timestamp of the last message that Singer uploads.
   *
   * Singer may have multiple log streams that belong to the same log. For instance, on ngapp
   * instances, we have 32 event_* log streams. All of these event_* log streams belong to the
   * "data.event_log" log stream. Because of this, in this method, we maintain a
   * [String -> List[long]] map on the processing latency of each log stream, and reports
   * the maximum value in the list.
   */
  public void reportStats() {
    // Now, we check stats for those streams which survive the cleanup.
    long currentTimeMillis = System.currentTimeMillis();
    int numStuckStreams = 0;
    long processingLatency;

    Map<String, List<Long>> perLogLatency = Maps.newHashMap();
    Map<String, Integer> perLogStuck = Maps.newHashMap();
    DefaultLogStreamProcessor processor;

    for (LogStream logStream : processedLogStreams.keySet()) {
      String logName = logStream.getSingerLog().getSingerLogConfig().getName();
      if (!perLogLatency.containsKey(logName)) {
        perLogLatency.put(logName, new ArrayList<>());
      }
      long lastModificationTime = logStream.getLastStreamModificationTime();
      long latestMessageTime = logStream.getLatestProcessedMessageTime();
      processor = (DefaultLogStreamProcessor) processedLogStreams.get(logStream);
      long lastCompleteCycleTime = processor.getLastCompleteCycleTime();

      if (latestMessageTime > 0 || lastCompleteCycleTime > 0) {
        long latestPivotTimestamp = Math.max(latestMessageTime, lastCompleteCycleTime);
        processingLatency = Math.max(lastModificationTime - latestPivotTimestamp, 0);
        perLogLatency.get(logName).add(processingLatency);
      } else {
        // the stream hasn't seen a single successful cycle so far.
        long stuckTime = currentTimeMillis - logStream.getCreationTime();
        if (stuckTime > MINIMUM_STUCK_STREAM_AGE_SECS * 1000) {
          numStuckStreams++;
          LOG.error("Singer critical alerts: Log stream {} has been stuck for {} milliseconds",
              logStream.getLogStreamDescriptor(), stuckTime);
          if (!perLogStuck.containsKey(logName)) {
            perLogStuck.put(logName, 1);
          } else {
            Integer val = perLogStuck.get(logName);
            perLogStuck.put(logName, val + 1);
          }
        }

        // for stuck stream, we use currentTime - logStreamCreation time as the latency
        // the previous computation using  currentTime - lastModificationTime(pevioius file)
        // in log stream can cause false warnings when singer restarts.
        perLogLatency.get(logName).add(stuckTime);
      }
    }

    Stats.setGauge("singer.processor.stuck_processors", numStuckStreams);
    long overallMaxLatency = 0;
    for (String log : perLogLatency.keySet()) {
      List<Long> latencyList = perLogLatency.get(log);
      if (latencyList.size() > 0) {
        long maxLatency = Collections.max(latencyList);
        overallMaxLatency = Math.max(overallMaxLatency, maxLatency);
        // colon ':' is not a supported character in OpenTSDB so we need to make it stats compliant
        OpenTsdbMetricConverter.addMetric(SingerMetrics.PROCESSOR_LATENCY, (int) maxLatency, "log=" + log.replace(":", "/"));
        Stats.setGauge(OpenTsdbMetricConverter.nameMetric(SingerMetrics.CURRENT_PROCESSOR_LATENCY,
            "log=" + log.replace(":", "/")), maxLatency);
      }
    }
    for (String log : perLogStuck.keySet()) {
      OpenTsdbMetricConverter
          .incr("singer.processor.stuck", perLogStuck.get(log), "log=" + log, "host=" + HOSTNAME);
    }
  }

  private void cleanUpLogs() {
    List<LogStream> emptyLogStreams = Lists.newArrayListWithCapacity(processedLogStreams.size());
    // Get all empty log streams.
    for (LogStream logStream : processedLogStreams.keySet()) {
      if (logStream.isEmpty()) {
        emptyLogStreams.add(logStream);
      }
    }

    if (emptyLogStreams.size() > 0) {
      LOG.info("The following log streams are empty: {}", Joiner.on(',').join(emptyLogStreams));
      // Unregister all empty log streams from monitor.
      List<LogStream> logStreamsCleanedUp =
          Lists.newArrayListWithExpectedSize(emptyLogStreams.size());
      for (LogStream emptyLogStream : emptyLogStreams) {
        try {
          LogStreamProcessor processor = processedLogStreams.get(emptyLogStream);
          processor.stop();
          processor.close();
          processedLogStreams.remove(emptyLogStream);

          String streamName = emptyLogStream.getLogStreamName();
          LOG.info("Closed log processor for empty log stream: " + streamName);
          logStreamsCleanedUp.add(emptyLogStream);

          LogStreamManager.removeLogStream(emptyLogStream);
        } catch (IOException e) {
          LOG.error("Caught IOException while closing an empty stream: " + emptyLogStream
              .getLogStreamName(), e);
        } catch (Exception e) {
          LOG.error("Caught unexpected exception", e);
          Stats.incr("singer.monitor.unexpected_exception");
        }
      }
      LOG.info("Empty log streams are cleaned up: {}", Joiner.on(',').join(logStreamsCleanedUp));
    }
  }

  private void stopMonitoredLogs() {
    for (Iterator<Map.Entry<LogStream, LogStreamProcessor>> it
         = processedLogStreams.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<LogStream, LogStreamProcessor> entry = it.next();
      LogStream logStream = entry.getKey();
      // Stop and close the processor for this LogStream.
      LogStreamProcessor processor = entry.getValue();
      processor.stop();
      try {
        processor.close();
        LOG.info("Stop and close processor for log stream: {}", logStream);
      } catch (IOException e) {
        LOG.error("Failed to close processor for log stream: {}", logStream);
      }
      // Remove the LogStream from processed LogStreams.
      it.remove();
    }
  }

  /**
   * Expand a name by replacing placeholder (such as \1, \2) in the name with captured group
   * from LogStream name.
   *
   * @param logStreamName the LogStream name this string will be expanded in.
   * @param streamRegex   the stream name regex.
   * @param name          the name to be expanded.
   * @return expanded name with placeholder replaced.
   * @throws ConfigurationException
   */
  public static String extractTopicNameFromLogStreamName(
      String logStreamName, String streamRegex, String name) throws ConfigurationException {
    try {
      Pattern pattern = Pattern.compile(streamRegex);
      Matcher logStreamNameMatcher = pattern.matcher(logStreamName);
      Preconditions.checkState(logStreamNameMatcher.matches());

      // Replace all group numbers in "name" with the groups from logStreamName.
      Pattern p = Pattern.compile("\\\\(\\d{1})");
      Matcher groupMatcher = p.matcher(name);
      StringBuffer sb = new StringBuffer();
      while (groupMatcher.find()) {
        groupMatcher.appendReplacement(
            sb,
            logStreamNameMatcher.group(Integer.parseInt(groupMatcher.group(1))));
      }
      groupMatcher.appendTail(sb);
      return sb.toString();
    } catch (NumberFormatException e) {
      throw new ConfigurationException("Cannot expand " + name + " in log stream " + logStreamName,
          e);
    }
  }
}
