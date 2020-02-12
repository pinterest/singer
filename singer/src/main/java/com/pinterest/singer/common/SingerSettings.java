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

import com.pinterest.singer.common.errors.SingerLogException;
import com.pinterest.singer.config.SingerDirectoryWatcher;
import com.pinterest.singer.environment.Environment;
import com.pinterest.singer.environment.EnvironmentProvider;
import com.pinterest.singer.heartbeat.HeartbeatGenerator;
import com.pinterest.singer.kubernetes.KubeService;
import com.pinterest.singer.loggingaudit.client.LoggingAuditClient;
import com.pinterest.singer.monitor.FileSystemMonitor;
import com.pinterest.singer.monitor.LogStreamManager;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.writer.KafkaProducerMetricsMonitor;
import com.twitter.ostrich.stats.Stats;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * SingerSettings encapsulate the global variables (thread pools, etc.) that are used in Singer.
 */
public final class SingerSettings {

  private static final int SINGER_STARTING_INDICATOR_DURATION_IN_MINUTES = 5;

  private static final Logger LOG = LoggerFactory.getLogger(SingerSettings.class);

  /**
   * The thread pool for processing the log streams
   */
  private static ScheduledExecutorService logProcessorExecutor = null;

  /**
   * The thread pools for writing the logs to central storage such as kafka. We use a thread
   * pool per cluster to isolate logging failures in one cluster from another.
   */
  private static Map<String, ExecutorService> logWritingExecutors = null;

  /**
   * Scheduled Executor for background task
   */
  private static ScheduledExecutorService backgroundTaskExecutor;

  /**
   * The Singer config file directory watcher
   */
  public static SingerDirectoryWatcher directoryWatcher;

  private static SingerConfig singerConfig = null;

  private static LogMonitor logMonitor;

  // Note the getOrCreate method is synchronized so this datastructure doesn't need to be threadsafe
  // There are no iterations performed on this datastructure
  private static Map<String, FileSystemMonitor> fsMonitorMap = new HashMap<>();

  public static HeartbeatGenerator heartbeatGenerator;
  
  // initialized here so that unit tests aren't required to call the initialize method below
  private static SortedMap<String, Collection<SingerLogConfig>> logConfigMap = new TreeMap<>();
  
  //environment is production by default
  private static Environment environment = new Environment();

  // loggingAuditClient is used to send LoggingAuditEvent if LoggingAudit feature is enabled and
  // a TopicAuditConfig is set for a given logStream.
  private static LoggingAuditClient loggingAuditClient = null;
  
  private static Thread kafkaProducerMonitorThread;

  private SingerSettings() {
  }

  public static void initialize(SingerConfig config)
      throws ClassNotFoundException,
             InvocationTargetException,
             IOException,
             IllegalAccessException,
             NoSuchMethodException,
      SingerLogException {
    setSingerConfig(config);

    loadAndSetSingerEnvironmentIfConfigured(config);
    LOG.warn("Singer environment has been configured to:" + environment);

    loadAndSetLoggingAuditClientIfEnabled(config);
    
    SingerSettings.logProcessorExecutor = Executors.newScheduledThreadPool(
        singerConfig.getThreadPoolSize(),
        new ThreadFactoryBuilder().setNameFormat("Processor: %d").build());

    SingerSettings.logWritingExecutors = new HashMap<>();
    
    backgroundTaskExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("Background-Task-Executor").build());
    
    // This is metric will help us track how frequently is singer getting restarted
    Stats.setGauge(SingerMetrics.SINGER_START_INDICATOR, 1);
    backgroundTaskExecutor.schedule(()->Stats.setGauge(SingerMetrics.SINGER_START_INDICATOR, 0), SINGER_STARTING_INDICATOR_DURATION_IN_MINUTES, TimeUnit.MINUTES);
    // We can alert on too many singer restarts on a given host so we can catch bad Singer code / too many exceptions 
    
    initializeConfigMap(config);

    if (config.isSetLogConfigs()) {
      // configure cluster signatures for each configured log cluster
      for (SingerLogConfig logConfig : config.getLogConfigs()) {
        String clusterSig = "";

        try {
          clusterSig = logConfig.getLogStreamWriterConfig().getKafkaWriterConfig()
              .getProducerConfig().getKafkaClusterSignature();
        } catch (Exception e) {
          LOG.warn("Unable to load kafka cluster signature from producer config reason:" + e.getMessage());
        }

        if (!logWritingExecutors.containsKey(clusterSig)) {
          // include cluster signature in the threadname to debug potential threading issues
          String threadName = clusterSig.replaceAll("/", "-");
          ExecutorService threadPool = Executors.newFixedThreadPool(
              singerConfig.getWriterThreadPoolSize(),
              new ThreadFactoryBuilder().setNameFormat("LogWriter:" + threadName + ": %d").build());

          LOG.debug("Initialized writer thread pool with {} as cluster signature.", clusterSig);

          logWritingExecutors.put(clusterSig, threadPool);
        }
        
        kafkaProducerMonitorThread = new Thread(new KafkaProducerMetricsMonitor());
        kafkaProducerMonitorThread.setDaemon(true);
        kafkaProducerMonitorThread.start();

        if (loggingAuditClient != null && logConfig.isEnableLoggingAudit() &&
            logConfig.getAuditConfig() != null){
          loggingAuditClient.addAuditConfig(logConfig.getName(), logConfig.getAuditConfig());
        }
      }
    }

    if (singerConfig != null
        && singerConfig.isSetHeartbeatEnabled() && singerConfig.isHeartbeatEnabled()) {
      heartbeatGenerator = new HeartbeatGenerator(singerConfig);
      logProcessorExecutor.scheduleAtFixedRate(heartbeatGenerator, 0L,
          singerConfig.getHeartbeatIntervalInSeconds(), TimeUnit.SECONDS);
    }

    // FileSystemMonitor registers watchKeys with log directories in the constructor.
    // After FileSystemMonitor.start() method is called, it will start examine the received
    // file system events, update inode mapping for files, and deal with new log streams.
    // Before doing this work, we need to start LogMonitor and have it initialize
    // the log streams. Otherwise, if FileSystemMonitor discovers a new file that matches a
    // log stream, it will fail to update the log stream.
    // The reason for this call is to start FSM if it's the regular singer environment before
    // LogStreamManager starts making calls; else we will miss FS events.

    // It's simply used to prime and start the thread; I could break it into 2 method calls: 
    // create object and then start but then we will need to call start everywhere we call get 
    // or create so I combined them into one.
    
    // start regular singer
    FileSystemMonitor globalFsm = new FileSystemMonitor(singerConfig, LogStreamManager.NON_KUBERNETES_POD_ID);
    fsMonitorMap.put(LogStreamManager.NON_KUBERNETES_POD_ID, globalFsm);
    
    if (singerConfig != null ) {
      LogStreamManager.initializeLogStreams();
      // check and start kubernetes singer
      if (singerConfig.isKubernetesEnabled()) {
        KubeService instance = KubeService.getInstance();
        instance.start();
      }
    }
    
    if (singerConfig != null && singerConfig.isSetLogMonitorConfig()) {
      LOG.info("Log monitor started");
      int monitorIntervalInSecs = singerConfig.getLogMonitorConfig().getMonitorIntervalInSecs();
      String monitorClassName = singerConfig.getLogMonitorClass();
      Method getInstanceMethod = getLogMonitorStaticInstanceMethod(monitorClassName);
      logMonitor = (LogMonitor) getInstanceMethod.invoke(null, monitorIntervalInSecs, singerConfig);
      logMonitor.start();
    }

    globalFsm.start();
  }

  protected static void loadAndSetLoggingAuditClientIfEnabled(SingerConfig config) {
    if (config.isLoggingAuditEnabled() && config.getLoggingAuditClientConfig() != null) {
      try {
        loggingAuditClient = new LoggingAuditClient(config.getLoggingAuditClientConfig());
        LOG.info("LoggingAudit client has been created.");
      } catch (Exception e) {
        LOG.error("Failed to create LoggingAuditClient.", e);
      }
    }
  }


  public static Method getLogMonitorStaticInstanceMethod(String monitorClassName) throws ClassNotFoundException,
                                                           NoSuchMethodException {
    Class<?> monitorClass = Class.forName(monitorClassName);
    Method getInstance = monitorClass.getMethod("getInstance", int.class, SingerConfig.class);
    return getInstance;
  }

  public static SortedMap<String, Collection<SingerLogConfig>> loadLogConfigMap(SingerConfig config) {
    SortedMap<String, Collection<SingerLogConfig>> logConfigMap = new TreeMap<>();
    if(config.getLogConfigs()!=null) {
        for (SingerLogConfig singerLogConfig : config.getLogConfigs()) {
            Collection<SingerLogConfig> collection = logConfigMap.get(singerLogConfig.getLogDir());
            if (collection == null) {
                collection = new ArrayList<>();
                logConfigMap.put(singerLogConfig.getLogDir(), collection);
            }
            collection.add(singerLogConfig);
        }
    }
    return logConfigMap;
  }

  public static synchronized FileSystemMonitor getOrCreateFileSystemMonitor(String podUid) throws SingerLogException {
    FileSystemMonitor mon = fsMonitorMap.get(podUid);
    if (mon == null) {
      try {
        mon = new FileSystemMonitor(singerConfig, podUid);
        fsMonitorMap.put(podUid, mon);
        
        LOG.info("Created and started FileSystemMonitor for "+podUid);
        mon.start();
      } catch (IOException e) {
        throw new SingerLogException("Error configuring and starting filesystem monitor for pod:" + podUid, e);
      }
    }
    return mon;
  }
  
  protected static void loadAndSetSingerEnvironmentIfConfigured(SingerConfig config) {
    if (config.getEnvironmentProviderClass() != null) {
      try {
        String environmentProviderClass = config.getEnvironmentProviderClass();
        @SuppressWarnings("unchecked")
        Class<EnvironmentProvider> providerClass = (Class<EnvironmentProvider>) 
            Class.forName(environmentProviderClass);
        EnvironmentProvider provider = providerClass.newInstance();
        Environment env = provider.getEnvironment();
        if (env != null) {
          environment = env;
          return;
        }
      } catch (Exception e) {
        LOG.error("Failed to load Singer Environment configuration", e);
      }
    }
  }
  
  public static Map<String, FileSystemMonitor> getFsMonitorMap() {
    return fsMonitorMap;
  }
  
  public static LogMonitor getLogMonitor() {
    return logMonitor;
  }
  
  public static Map<String, ExecutorService> getLogWritingExecutors() {
    return logWritingExecutors;
  }
  
  public static SingerConfig getSingerConfig() {
    return singerConfig;
  }
  
  public static void setSingerConfig(SingerConfig singerConfig) {
    SingerSettings.singerConfig = singerConfig;
  }
  
  public static void reset() {
    for(Entry<String, FileSystemMonitor> mon:fsMonitorMap.entrySet()) {
        mon.getValue().stop();
    }
    if(logMonitor!=null) {
       logMonitor.stop();
	   logMonitor = null;
    }
    if(backgroundTaskExecutor!=null) {
        backgroundTaskExecutor.shutdownNow();
    }
    singerConfig = null;
    fsMonitorMap.clear();
  }
  
  public static SortedMap<String, Collection<SingerLogConfig>> getLogConfigMap() {
    return logConfigMap;
  }
  
  public static ScheduledExecutorService getBackgroundTaskExecutor() {
    return backgroundTaskExecutor;
  }
  
  public static void setBackgroundTaskExecutor(ScheduledExecutorService backgroundTaskExecutor) {
    SingerSettings.backgroundTaskExecutor = backgroundTaskExecutor;
  }
  
  public static ScheduledExecutorService getLogProcessorExecutor() {
    return logProcessorExecutor;
  }
  
  /**
   * Refactored so unit tests can call
   * @param config
   */
  public static void initializeConfigMap(SingerConfig config) {
    logConfigMap = loadLogConfigMap(config);
  }
  
  public static Environment getEnvironment() {
    return environment;
  }
  
  @VisibleForTesting
  public static void setEnvironment(Environment environment) {
    SingerSettings.environment = environment;
  }

  public static LoggingAuditClient getLoggingAuditClient() {
    return loggingAuditClient;
  }

  public static void setLoggingAuditClient(LoggingAuditClient loggingAuditClient) {
    SingerSettings.loggingAuditClient = loggingAuditClient;
  }
  
  public static Thread getKafkaProducerMonitorThread() {
    return kafkaProducerMonitorThread;
  }
}
