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

import com.pinterest.singer.thrift.configuration.SingerConfig;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Watcher periodically re-parse entire directory tree and exits the Singer process if any change
 * detected.
 */
public class SingerDirectoryWatcher implements Runnable {

  interface ExitManager {

    void exit(int status);
  }

  static final ExitManager systemExitManager = new ExitManager() {
    @Override
    public void exit(int status) {
      System.exit(status);
    }
  };
  private final ExitManager exitManager;
  private final SingerConfig origConfig;
  private final SingerConfigurator configurator;
  private static final Logger LOG = LoggerFactory.getLogger(SingerDirectoryWatcher.class);

  public SingerDirectoryWatcher(SingerConfig origConfig, SingerConfigurator singerConfigurator)
      throws ConfigurationException {
    this(origConfig, singerConfigurator, systemExitManager);
  }

  @VisibleForTesting
  public SingerDirectoryWatcher(SingerConfig origConfig,
                                SingerConfigurator singerConfigurator,
                                ExitManager exitManager) throws ConfigurationException {
    this.origConfig = origConfig;
    this.configurator = singerConfigurator;
    this.exitManager = exitManager;
    // start a daemon thread so we can exit without any bothering.
    ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(new
        ThreadFactoryBuilder().setDaemon(true).setNameFormat("SingerDirectoryWatcher-%d").build());
    service.scheduleAtFixedRate(this, 0, origConfig.getLogConfigPollIntervalSecs(),
        TimeUnit.SECONDS);
  }

  @Override
  public void run() {
    try {
      SingerConfig newConfig = configurator.parseSingerConfig();
      if (!origConfig.equals(newConfig)) {
        LOG.error("Exiting as singer config changes detected. This is by design, don't panic.");
        exitManager.exit(0);
      }
    } catch (ConfigurationException e) {
      LOG.error("Fail to reloading Singer config dir due to exception: {}. Exiting!",
          ExceptionUtils.getFullStackTrace(e));
      exitManager.exit(1);
    }
  }
}
