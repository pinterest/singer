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

import com.google.common.base.Preconditions;
import com.pinterest.singer.writer.KafkaMessagePartitioner;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.common.PartitionInfo;

/**
 * This partitioner will produce true round-robin assignments by
 * using a simple modhash thus avoiding the need to do any checksum
 * hash computation.
 * 
 * Thread safety is not a concern here since KafkaWriter instantiates it.
 * 
 * NOTE: this class is NOT threadsafe
 */
@NotThreadSafe
public class SimpleRoundRobinPartitioner implements KafkaMessagePartitioner {

  private long counter = 0;

  public SimpleRoundRobinPartitioner() {
  }

  public int partition(Object object, List<PartitionInfo> partitions) {
    int numOfPartitions = partitions.size();
    Preconditions.checkArgument(numOfPartitions > 0);
    int partition = (int) (counter % numOfPartitions);
    counter++;
    return partition;
  }
  
}
