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
package com.pinterest.singer.utils;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.pinterest.singer.config.ServersetMonitor;

public final class BrokerSetChangeListener implements ServersetMonitor {
  
  private static final int RESTART_WAIT_MAX_DELAY = 1800_000;
  private static final int BROKER_RESTART_PRECENTAGE_THRESHOLD = 50;
  private static final int BROKER_RESTART_VALUE_THRESHOLD = 1;
  private static final Logger LOG = LoggerFactory.getLogger(BrokerSetChangeListener.class);
  private final String monitoredServersetFilePath;
  private ConcurrentMap<String, Set<String>> kafkaServerSets;
  private int restartTimeLimit = RESTART_WAIT_MAX_DELAY;

  public BrokerSetChangeListener(String monitoredServersetFilePath,
      ConcurrentMap<String, Set<String>> kafkaServerSets) {
    this.monitoredServersetFilePath = monitoredServersetFilePath;
    this.kafkaServerSets = kafkaServerSets;
  }

  public BrokerSetChangeListener(String monitoredServersetFilePath,
      ConcurrentMap<String, Set<String>> kafkaServerSets, int randomTimeLimit) {
    this.monitoredServersetFilePath = monitoredServersetFilePath;
    this.kafkaServerSets = kafkaServerSets;
    this.restartTimeLimit = randomTimeLimit;
  }

  @Override
  public void onChange(ImmutableSet<InetSocketAddress> serviceInstances) {
    Set<String> newBrokerSet = Sets.newHashSet();
    for (InetSocketAddress instance : serviceInstances) {
      newBrokerSet.add(Joiner.on(":").join(instance.getHostName(), instance.getPort()));
    }
    // 'serverSetPath' variable will still long live. But it should be rare as we
    // only
    // reach here when see a new serverSetPath.
    Set<String> oldBrokerSet = kafkaServerSets.putIfAbsent(monitoredServersetFilePath,
        newBrokerSet);
    if (oldBrokerSet != null) {
      int delta = Sets.difference(oldBrokerSet, newBrokerSet).size();

      double percentageChange = delta * 100 / oldBrokerSet.size();
      // On serverSet update, we exit the agent, assuming external process manager
      // will restart the agent to pick up new file content. Singer doesn't rely on
      // in-memory state (with all log processing watermark on disk), so exiting
      // the agent here is *safe*.

      // Restart randomly in 1800 seconds after metadata change
      // Percentage threshold is added for small clusters
      // We delay for a restart to avoid causing connection and data spikes on the Kafka clusters
      if (delta > BROKER_RESTART_VALUE_THRESHOLD || percentageChange > BROKER_RESTART_PRECENTAGE_THRESHOLD) {
        try {
          Thread.sleep(ThreadLocalRandom.current().nextInt(restartTimeLimit));
        } catch (InterruptedException e) {
          LOG.warn("Broker config restart interrupted");
        }
        LOG.warn("Exiting on kafka broker serverset update for {}", monitoredServersetFilePath);
        System.exit(0);
      }
    }
  }
}