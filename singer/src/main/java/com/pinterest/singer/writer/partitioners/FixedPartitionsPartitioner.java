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

import com.pinterest.singer.writer.KafkaMessagePartitioner;

import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * This partition is used to group a batch of messages into one of fixed number of partitions
 * This will potentially help out with both the compression ratio and connection count.
 * The number of partitions should be chosen as a multiplier for a reasonable producer-to-partition ratio.
 * Default behaviour (with no configs) would be similar to SinglePartitionPartitioner
 */
public class FixedPartitionsPartitioner implements KafkaMessagePartitioner {
  public static final String PARTITIONER_PARTITION_COUNT_KEY = "partitioner.partitionCount";
  private static final Logger logger = LoggerFactory.getLogger(FixedPartitionsPartitioner.class);

  private final Map<Integer, List<Integer>> partitionMap = new HashMap<>();
  private int partitionCount = 1;


  @Override
  public void configure(Map<String, String> partitionerConfig) {
    if (partitionerConfig != null) {
      KafkaMessagePartitioner.super.configure(partitionerConfig);
      partitionCount = Integer.parseInt(partitionerConfig.getOrDefault(PARTITIONER_PARTITION_COUNT_KEY, "1"));
      logger.info("Partitioner configured to partition to at most " + partitionCount + " partitions");
    }
  }

  public int partition(Object object, List<PartitionInfo> partitions) {
    int numOfPartitions = partitions.size();

    /* For each partition count during Singer's lifetime, we derive a fixed set of different partitions for
       that partition count of the topic to reduce the number of brokers this host reaches out to.
     */
    if (!partitionMap.containsKey(numOfPartitions)){
      List<Integer> selectedPartitions = new ArrayList<>();
      while (selectedPartitions.size() < Math.min(partitionCount, numOfPartitions)) {
        int selectedPartition = Math.abs(ThreadLocalRandom.current().nextInt(numOfPartitions));
        while (selectedPartitions.contains(selectedPartition)) {
          selectedPartition = Math.abs(ThreadLocalRandom.current().nextInt(numOfPartitions));
        }
        selectedPartitions.add(selectedPartition);
      }
      partitionMap.put(numOfPartitions, selectedPartitions);
      logger.info("For " + numOfPartitions + " partitions, delivering to partitions " + selectedPartitions.stream().map(Object::toString).collect(Collectors.joining(" ")));
    }
    List<Integer> listOfPartitions = partitionMap.get(numOfPartitions);
    return listOfPartitions.get(ThreadLocalRandom.current().nextInt(listOfPartitions.size()));
  }
}
