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

import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Locality aware partitioner single partition partitioner. This partitioner can
 * heuristically provide better compression ratios for data since all data from
 * a given singer agent is written to one partition. For some datasets we have
 * seen ~2x compression ratios.
 */
public class LocalityAwareSinglePartitionPartitioner extends LocalityAwarePartitioner {

  // refresh every 30 seconds
  public static final long REFRESH_INTERVAL_MS = 30_000;
  private ThreadLocalRandom random = ThreadLocalRandom.current();
  private int partitionId;

  public LocalityAwareSinglePartitionPartitioner() {
    super(REFRESH_INTERVAL_MS);
  }

  protected LocalityAwareSinglePartitionPartitioner(String rack, long refreshIntervalMs) {
    super(rack, refreshIntervalMs);
  }

  @Override
  public int partition(Object messageKey, List<PartitionInfo> partitions) {
    if (localPartitions == null || isTimeToRefresh()) {
      checkAndAssignLocalPartitions(partitions);
      // set next refresh time
      updateNextRefreshTime();
      // NOTE we are not doing a delta update here since PartitionInfo object doesn't
      // have an overridden hashcode and equals implementation therefore the delta computation
      // will be cumbersome
      partitionId = localPartitions.get(Math.abs(random.nextInt() % localPartitions.size()))
          .partition();
    }
    return partitionId;
  }

}