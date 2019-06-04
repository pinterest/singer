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

import com.pinterest.singer.common.SingerConfigDef;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.utils.SingerUtils;

import com.twitter.ostrich.stats.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;

public class FileSystemEventFetcher implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemEventFetcher.class);

  private LinkedBlockingQueue<FileSystemEvent> fileSystemEvents;

  private WatchService watchService;
  private final Map<WatchKey, Path> keys = new HashMap<>();
  // Register path can be called asynchronously
  private final Set<Path> monitoredPaths = new ConcurrentSkipListSet<>();

  private boolean cancelled = true;
  private Thread thread;
  private String name;

  public FileSystemEventFetcher() throws IOException {
    fileSystemEvents = new LinkedBlockingQueue<>();
    watchService = FileSystems.getDefault().newWatchService();
  }

  public FileSystemEvent getEvent() throws InterruptedException {
    FileSystemEvent event = fileSystemEvents.take();
    return event;
  }

  public Set<Path> getMonitoredPaths() {
    return this.monitoredPaths;
  }

  /**
   * Registers a path with the WatchService so we will be notified of changes in its files
   * Note that WaterService may generate ENTRY_MODIFY event for file rename.
   * For instance, if we have rename "a.1 -> a.2, a ->a.1", WatchService generate 3 events
   *     ENTRY_MODIFY a.1
   *     ENTRY_DELETE a
   *     ENTRY_CREATE a.2
   *
   * @param logDir A directory containing log files that we want to track
   * @throws IOException If there is a problem reading the directory or registering the watch
   * service
   */
  public void registerPath(Path logDir) throws IOException {
    if (!logDir.toFile().isDirectory()) {
      LOG.error("Invalid log directory : {}", logDir.toAbsolutePath());
      Stats.incr(SingerMetrics.IO_EXCEPTION_INVALID_DIR);
    } else if (!monitoredPaths.contains(logDir)) {
      WatchKey watchKey = SingerUtils.registerWatchKey(watchService, logDir);
      keys.put(watchKey, logDir);
      monitoredPaths.add(logDir);
      LOG.info("Monitoring paths in " + logDir.toString());
    }
  }

  /**
   *  Start a file system event fetching thread. This method is idempotent.
   *  @param name of the event fetcher.
   */
  public void start(String name) {
    // we don't want to start another thread for the 
    // same instance of FileSystemEventFetcher
    if(this.thread == null) {
        this.name = name;
        thread = new Thread(this);
        thread.setName("FileSystemEventFetcher-" + name);
        thread.start();
    }
  }

  /**
   * This method is idempotent
   */
  public void stop() {
    cancelled = true;
    if(this.thread != null && thread.isAlive()) {
        thread.interrupt();
    }
  }

  @Override
  public void run() {
    cancelled = false;
    try {
      while (!cancelled) {
        try {
          LOG.debug("Start checking file system events");
          // Blocks until keys are present
          WatchKey watchKey = watchService.take();
          Path logDir = keys.get(watchKey);

          for (final WatchEvent<?> event : watchKey.pollEvents()) {
            fileSystemEvents.add(new FileSystemEvent(logDir, event));
            OpenTsdbMetricConverter.incr(SingerMetrics.FS_EVENT, 1, "kind=" + event.kind().toString());
          }
          boolean isValidWatchKey = watchKey.reset();
          if (!isValidWatchKey) {
            try {
              WatchKey newWatchKey = SingerUtils.registerWatchKey(watchService, logDir);
              keys.put(newWatchKey, logDir);
            } catch (IOException e) {
              LOG.error("WatchKey for " + watchKey.toString() + " is invalid and could not make a new one");
              Stats.incr(SingerMetrics.IO_EXCEPTION_METRIC_NAME);
            }
            watchKey.cancel();
          }
          int numEvents = fileSystemEvents.size();
          OpenTsdbMetricConverter.addMetric(SingerMetrics.FS_EVENT_QUEUE_SIZE, numEvents);
        } catch (InterruptedException e) {
          LOG.error("Filesystem Monitor thread was interrupted: {}");
          if (cancelled) {
            break;
          }
        }
      }
    } catch (Exception e) {
        // print all non-interrupted exceptions that aren't caught
        LOG.error("Exception in FileSystemEventFetcher, exiting loop", e);
        System.exit(SingerConfigDef.SINGER_EXIT_FSEF_EXCEPTION);
    } finally {
      try {
        watchService.close();
      } catch (IOException e) {
        LOG.error("Unable to shutdown watch service: ", e);
      }
      LOG.warn("FileSystemEventFetcher " + name + " stopping");
    }
  }
}