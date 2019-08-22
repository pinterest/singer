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
package com.pinterest.singer.writer.partitioners;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.writer.KafkaMessagePartitioner;
import com.pinterest.singer.writer.KafkaWriter;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Locality (aws ec2 az) aware Kafka partitioner. This partitioner attempts to
 * write to partitions whose leaders are in the same locality as the Kafka
 * leader for the partition.
 */
public abstract class LocalityAwarePartitioner implements KafkaMessagePartitioner {

  // the locality info is constant for a given instance of Singer and will
  // not change during the Singer process lifetime for a given host
  protected static String rack;
  private long nextRefreshTime;
  private long refreshIntervalMs;
  protected List<PartitionInfo> localPartitions;

  public LocalityAwarePartitioner(long refreshIntervalMs) {
    this.refreshIntervalMs = refreshIntervalMs;
    // do a singleton based implementation
    if (rack == null) {
      synchronized (LocalityAwarePartitioner.class) {
        if (rack == null) {
          // load rack information from SingerEnvironment
          rack = SingerSettings.getEnvironment().getLocality();
        }
      }
    }
  }

  protected LocalityAwarePartitioner(String rack, long refreshIntervalMs) {
    LocalityAwarePartitioner.rack = rack;
    this.refreshIntervalMs = refreshIntervalMs;
  }

  protected void updateNextRefreshTime() {
    nextRefreshTime = System.currentTimeMillis() + refreshIntervalMs;
  }

  /**
   * Retain partitions that are local to this agent
   * @param partitions
   * @return local partitions
   */
  protected List<PartitionInfo> retainLocalPartitions(List<PartitionInfo> partitions) {
    return partitions.stream()
        .filter(p -> p.leader() != null && p.leader().hasRack() && rack.equals(p.leader().rack()))
        .collect(Collectors.toList());
  }
  
  /**
   * Check if local partitions are available if yes then assign those to the localPartitions object
   * else assigns all supplied partitions to the localPartitions object
   * @param partitions
   */
  protected void checkAndAssignLocalPartitions(List<PartitionInfo> partitions) {
    List<PartitionInfo> retainLocalPartition = retainLocalPartitions(partitions);
    if (retainLocalPartition.isEmpty()) {
      OpenTsdbMetricConverter.gauge(SingerMetrics.MISSING_LOCAL_PARTITIONS, 1, "locality=" + rack,
          "host=" + KafkaWriter.HOSTNAME);
      // reset to all partitions if no local partitions are available
      localPartitions = partitions;
    } else {
      localPartitions = retainLocalPartition;
    }
  }

  protected boolean isTimeToRefresh() {
    return System.currentTimeMillis() > nextRefreshTime;
  }

  protected void setNextRefreshTime(long nextRefreshTime) {
    this.nextRefreshTime = nextRefreshTime;
  }

  public List<PartitionInfo> getLocalPartitions() {
    return localPartitions;
  }

  public String getRack() {
    return rack;
  }

  @VisibleForTesting
  protected static void nullifyRack() {
    rack = null;
  }

  public long getRefreshIntervalMs() {
    return refreshIntervalMs;
  }
}