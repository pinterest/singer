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

import com.pinterest.singer.common.SingerLog;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.common.errors.SingerLogException;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.ostrich.stats.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MissingDirChecker is a daemon thread that get initialized, started and stopped in
 * LogStreamManager.  When LogStreamManager tries to create LogStreams for each SingerLog, if the
 * corresponding log directory (eg: /mnt/thrift_logger/) has not been created yet, the SingerLog
 * will be put to a HashMap named singerLogsWithoutDir. MissingDirChecker will run every
 * sleepInMills (eg: 20000 milliseconds) and check if the log directory for this SingerLog has
 * been created. Once log directory has been created, LogStreamManager.initializeLogStreams method
 * will be called for this SingerLog and SingerLog will be removed from singerLogsWithoutDir if
 * the method call does not throw any exception.
 *
 */
public class MissingDirChecker implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(MissingDirChecker.class);

  private long sleepInMills = 20000L;
  private AtomicBoolean cancelled = new AtomicBoolean(false);
  private Map<SingerLog, String> singerLogsWithoutDir;
  private Thread thread;

  @VisibleForTesting
  public AtomicBoolean getCancelled() {
    return cancelled;
  }

  @VisibleForTesting
  public long getSleepInMills() {
    return sleepInMills;
  }

  @VisibleForTesting
  public void setSleepInMills(long sleepInMills) {
    this.sleepInMills = sleepInMills;
  }

  @VisibleForTesting
  public Map<SingerLog, String> getSingerLogsWithoutDir() {
    return singerLogsWithoutDir;
  }

  public void setSingerLogsWithoutDir(Map<SingerLog, String> singerLogsWithoutDir) {
    this.singerLogsWithoutDir = singerLogsWithoutDir;
  }

  @Override
  public void run() {
    LOG.info("[{}] is checking missing directories if any...", Thread.currentThread().getName());
    try {
      while (!cancelled.get() && singerLogsWithoutDir != null && singerLogsWithoutDir.size() >0) {
        LOG.info("[{}] found {} singerLogs without directory",
            Thread.currentThread().getName(), singerLogsWithoutDir.size());
        Iterator<Map.Entry<SingerLog, String>> iterator = singerLogsWithoutDir.entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry<SingerLog, String> entry = iterator.next();
          SingerLog singerLog = entry.getKey();
          String podUid = entry.getValue();
          SingerLogConfig singerLogConfig = singerLog.getSingerLogConfig();
          String logDir = singerLogConfig.getLogDir();
          File dir = new File(logDir);
          // if dir has been created, method initializeLogStreams will be called for this SingerLog.
          // This SingerLog will be removed once method call throws no exception.
          if (dir.exists()) {
            try {
              LogStreamManager.initializeLogStreams(singerLog, podUid);
              iterator.remove();
              Stats.setGauge(SingerMetrics.NUMBER_OF_MISSING_DIRS,  singerLogsWithoutDir.size());
              LOG.info("[{}] after {} created, log stream is initialized for {}.",
                  Thread.currentThread().getName(), dir.getAbsoluteFile(), singerLog.getLogName());
            } catch (SingerLogException e) {
              LOG.warn("Exception thrown while initializing log streams ", e);
            }
          }
        }
        LOG.info("[{}] sleep for {} milliseconds and then check again.",
            Thread.currentThread().getName(), sleepInMills);
        Thread.sleep(sleepInMills);
      }
    } catch (InterruptedException e){
      Stats.incr(SingerMetrics.MISSING_DIR_CHECKER_INTERRUPTED);
      LOG.warn("MissingDirChecker thread is interrupted ", e);
    } catch (Exception e){
      LOG.error("MissingDirChecker thread needs to stop due to ", e);
    } finally {
      LOG.info("MissingDirChecker thread stopped. SingerLogs without dir: " + singerLogsWithoutDir);
    }
  }

  public synchronized void start() {
    if(this.thread == null) {
      thread = new Thread(this);
      thread.setDaemon(true);
      thread.setName("MissingDirChecker");
      thread.start();
    }
  }

  public synchronized void stop(){
    cancelled.set(true);
    if(this.thread != null && thread.isAlive()) {
      thread.interrupt();
    }
  }

}
