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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.apache.kafka.common.PartitionInfo;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.singer.writer.KafkaMessagePartitioner;
import com.pinterest.singer.writer.kafka.localityaware.EC2LocalityInfoProvider;

/**
 * Locality (aws ec2 az) aware Kafka partitioner. This partitioner attempts to
 * write to partitions whose leaders are in the same locality as the Kafka
 * leader for the partition. If no partitions are available in the same
 * locality, a random partition from all partitions will be picked.
 */
public class LocalityAwareRandomPartitioner implements KafkaMessagePartitioner {

  // refresh every 10 seconds
  public static final long REFRESH_INTERVAL_MS = 10_000;
  // the locality info is constant for a given instance of Singer and will
  // not change during the Singer process lifetime for a given host
  private static String rack;
  private long nextRefreshTime;
  private long refreshIntervalMs;
  private List<PartitionInfo> localPartitions;
  private ThreadLocalRandom random = ThreadLocalRandom.current();

  public LocalityAwareRandomPartitioner() {
    refreshIntervalMs = REFRESH_INTERVAL_MS;
    // do a singleton based implementation on LocalityInfo provider instantiation
    if (rack == null) {
      synchronized (LocalityAwareRandomPartitioner.class) {
        if (rack == null) {
          EC2LocalityInfoProvider provider = new EC2LocalityInfoProvider();
          try {
            provider.init(new HashMap<>());
          } catch (IllegalArgumentException | IOException e) {
            throw new RuntimeException("Failed to initialize EC2 locality provider", e);
          }
          rack = provider.getLocalityInfoForLocalhost();
        }
      }
    }
  }

  protected LocalityAwareRandomPartitioner(String rack, long refreshIntervalMs) {
    LocalityAwareRandomPartitioner.rack = rack;
    this.refreshIntervalMs = refreshIntervalMs;
  }

  @Override
  public int partition(Object messageKey, List<PartitionInfo> partitions) {
    if (localPartitions == null || isTimeToRefresh()) {
      List<PartitionInfo> retainLocalPartition = retainLocalPartitions(partitions);
      localPartitions = retainLocalPartition.size() > 0 ? retainLocalPartition : partitions;
      // set next refresh time
      updateNextRefreshTime();
    }
    // we supply on local partitions to this base partitioner
    return localPartitions.get(Math.abs(random.nextInt() % localPartitions.size())).partition();
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