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
package com.pinterest.singer.config;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.common.base.MorePreconditions;
import com.twitter.util.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Class to monitor config files on local disk. Typical usage is to use the default instance
 * and have it monitor as many config files as needed.
 *
 * The class allows users to specify a watch on a file path and pass in a callback that
 * will be invoked whenever an update to the file is detected. Update detection currently
 * works by periodic polling; if the last modified time on the file is updated and the
 * content hash has changed, all watchers on that file are notified. Note that last modified
 * time is just used as a hint to determine whether to check the content and not for versioning.
 *
 * Objects of this class are thread safe.
 *
 */
public class ConfigFileWatcher {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigFileWatcher.class);
  private static final HashFunction HASH_FUNCTION = Hashing.md5();
  public static final int DEFAULT_POLL_PERIOD_SECONDS = 10;
  private static volatile ConfigFileWatcher DEFAULT_INSTANCE = null;

  // Thread safety note: only addWatch() can add new entries to this map, and that method
  // is synchronized. The reason for using a concurrent map is only to allow the watcher
  // thread to concurrently iterate over it.
  private final ConcurrentMap<String, ConfigFileInfo> watchedFileMap = Maps.newConcurrentMap();
  private final WatcherTask watcherTask;

  /**
   * Creates the default ConfigFileWatcher instance on demand.
   */
  public static ConfigFileWatcher defaultInstance() {
    if (DEFAULT_INSTANCE == null) {
      synchronized (ConfigFileWatcher.class) {
        if (DEFAULT_INSTANCE == null) {
          DEFAULT_INSTANCE = new ConfigFileWatcher(DEFAULT_POLL_PERIOD_SECONDS);
        }
      }
    }

    return DEFAULT_INSTANCE;
  }

  @VisibleForTesting
  ConfigFileWatcher(int pollPeriodSeconds) {
    ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ConfigFileWatcher-%d").build());
    this.watcherTask = new WatcherTask();
    service.scheduleWithFixedDelay(
        watcherTask, pollPeriodSeconds, pollPeriodSeconds, TimeUnit.SECONDS);
  }

  /**
   * Adds a watch on the specified file. The file must exist, otherwise a FileNotFoundException
   * is returned. If the file is deleted after a watch is established, the watcher will log errors
   * but continue to monitor it, and resume watching if it is recreated.
   *
   * @param filePath path to the file to watch.
   * @param onUpdate function to call when a change is detected to the file. The entire contents
   *                 of the file will be passed in to the function. Note that onUpdate will be
   *                 called once before this call completes, which facilities initial load of data.
   *                 This callback is executed synchronously on the watcher thread - it is
   *                 important that the function be non-blocking.
   */
  public synchronized void addWatch(String filePath, Function<byte[], Void> onUpdate)
      throws IOException {
    MorePreconditions.checkNotBlank(filePath);
    Preconditions.checkNotNull(onUpdate);

    // Read the file and make the initial onUpdate call.
    File file = new File(filePath);
    ByteSource byteSource = Files.asByteSource(file);
    onUpdate.apply(byteSource.read());

    // Add the file to our map if it isn't already there, and register the new change watcher.
    ConfigFileInfo configFileInfo = watchedFileMap.get(filePath);
    if (configFileInfo == null) {
      configFileInfo = new ConfigFileInfo(file.lastModified(), byteSource.hash(HASH_FUNCTION));
      watchedFileMap.put(filePath, configFileInfo);
    }
    configFileInfo.changeWatchers.add(onUpdate);
  }

  @VisibleForTesting
  public void runWatcherTaskNow() {
    watcherTask.run();
  }

  /**
   * Scheduled task that periodically checks each watched file for updates, and if found to have
   * been changed, triggers notifications on all its watchers.
   *
   * Thread safety note: this task must be run in a single threaded executor; i.e. only one run
   * of the task can be active at any time.
   */
  private class WatcherTask implements Runnable {

    @Override
    public void run() {
      for (Map.Entry<String, ConfigFileInfo> entry : watchedFileMap.entrySet()) {
        String filePath = entry.getKey();
        ConfigFileInfo configFileInfo = entry.getValue();
        try {
          File file = new File(filePath);
          long lastModified = file.lastModified();
          Preconditions.checkArgument(lastModified > 0L);
          if (lastModified != configFileInfo.lastModifiedTimestampMillis) {
            configFileInfo.lastModifiedTimestampMillis = lastModified;
            ByteSource byteSource = Files.asByteSource(file);
            HashCode newContentHash = byteSource.hash(HASH_FUNCTION);
            if (!newContentHash.equals(configFileInfo.contentHash)) {
              configFileInfo.contentHash = newContentHash;
              LOG.info("File {} was modified at {}, notifying watchers.", filePath, lastModified);
              byte[] newContents = byteSource.read();
              for (Function<byte[], Void> watchers : configFileInfo.changeWatchers) {
                try {
                  watchers.apply(newContents);
                } catch (Exception e) {
                  LOG.error(
                      "Exception in watcher callback for {}, ignoring. New file contents were: {}",
                      filePath, new String(newContents, Charsets.UTF_8), e);
                }
              }
            } else {
              LOG.info("File {} was modified at {} but content hash is unchanged.",
                  filePath, lastModified);
            }
          } else {
            LOG.debug("File {} not modified since {}", filePath, lastModified);
          }
        } catch (Exception e) {
          // We catch and log exceptions related to the update of any specific file, but
          // move on so others aren't affected. Issues can happen for example if the watcher
          // races with an external file replace operation; in that case, the next run should
          // pick up the update.
          // TODO: Consider adding a metric to track this so we can alert on failures.
          LOG.error("Config update check failed for {}", filePath, e);
        }
      }
    }
  }

  /**
   * Encapsulates state related to each watched config file.
   *
   * Thread safety note:
   *   1. changeWatchers is thread safe since it uses a copy-on-write array list.
   *   2. lastModifiedTimestampMillis and contentHash aren't safe to update across threads. We
   *      initialize in addWatch() at construction time, and thereafter only the watcher task
   *      thread accesses this state, so we are good.
   */
  private static class ConfigFileInfo {

    private final List<Function<byte[], Void>> changeWatchers = Lists.newCopyOnWriteArrayList();
    private long lastModifiedTimestampMillis;
    private HashCode contentHash;

    public ConfigFileInfo(long lastModifiedTimestampMillis, HashCode contentHash) {
      this.lastModifiedTimestampMillis = lastModifiedTimestampMillis;
      Preconditions.checkArgument(lastModifiedTimestampMillis > 0L);
      this.contentHash = Preconditions.checkNotNull(contentHash);
    }
  }
}
