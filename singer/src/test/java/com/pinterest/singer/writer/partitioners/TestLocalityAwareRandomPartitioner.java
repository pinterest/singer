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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;

public class TestLocalityAwareRandomPartitioner {

  @Test
  public void testDefaultInit() {
    LocalityAwareRandomPartitioner.nullifyRack();
    LocalityAwareRandomPartitioner partitioner = new LocalityAwareRandomPartitioner();
    assertNotNull(partitioner.getRack());
    assertEquals(LocalityAwareRandomPartitioner.REFRESH_INTERVAL_MS,
        partitioner.getRefreshIntervalMs());
  }

  @Test
  public void testRetainLocalPartitions() {
    List<PartitionInfo> partitions = Arrays.asList(
        new PartitionInfo("topix", 0, new Node(0, "0", 9092, "us-east-1a"), null, null),
        new PartitionInfo("topix", 1, new Node(0, "0", 9092, "us-east-1c"), null, null),
        new PartitionInfo("topix", 3, new Node(0, "0", 9092, "us-east-1d"), null, null),
        new PartitionInfo("topix", 2, new Node(0, "0", 9092, "us-east-1e"), null, null));
    LocalityAwareRandomPartitioner partitioner = new LocalityAwareRandomPartitioner("us-east-1e",
        10000);
    List<PartitionInfo> retainLocalPartition = partitioner.retainLocalPartitions(partitions);
    assertEquals(1, retainLocalPartition.size());
    assertEquals("us-east-1e", retainLocalPartition.get(0).leader().rack());

    partitioner = new LocalityAwareRandomPartitioner("us-east-1c", 10000);
    retainLocalPartition = partitioner.retainLocalPartitions(partitions);
    assertEquals(1, retainLocalPartition.size());
    assertEquals("us-east-1c", retainLocalPartition.get(0).leader().rack());

    partitions = Arrays.asList(
        new PartitionInfo("topix", 0, new Node(0, "0", 9092, "us-east-1a"), null, null),
        new PartitionInfo("topix", 1, new Node(0, "0", 9092, "us-east-1c"), null, null),
        new PartitionInfo("topix", 2, new Node(0, "0", 9092, "us-east-1d"), null, null),
        new PartitionInfo("topix", 4, new Node(0, "0", 9092, "us-east-1d"), null, null),
        new PartitionInfo("topix", 3, new Node(0, "0", 9092, "us-east-1e"), null, null));
    partitioner = new LocalityAwareRandomPartitioner("us-east-1d", 10000);
    retainLocalPartition = partitioner.retainLocalPartitions(partitions);
    assertEquals(2, retainLocalPartition.size());
    assertEquals("us-east-1d", retainLocalPartition.get(0).leader().rack());
    assertEquals(2, retainLocalPartition.get(0).partition());
    assertEquals(4, retainLocalPartition.get(1).partition());
  }

  @Test
  public void testPartition() throws InterruptedException {
    List<PartitionInfo> partitions = Arrays.asList(
        new PartitionInfo("topix", 0, new Node(0, "0", 9092, "us-east-1a"), null, null),
        new PartitionInfo("topix", 1, new Node(1, "0", 9092, "us-east-1e"), null, null),
        new PartitionInfo("topix", 3, new Node(2, "0", 9092, "us-east-1d"), null, null),
        new PartitionInfo("topix", 2, new Node(3, "0", 9092, "us-east-1e"), null, null));
    LocalityAwareRandomPartitioner partitioner = new LocalityAwareRandomPartitioner("us-east-1e",
        10000);

    assertEquals("us-east-1e", partitioner.getRack());
    // initialize local partitions
    partitioner.partition(String.valueOf(0), partitions);
    List<Integer> filteredPartitions = partitioner.getLocalPartitions().stream()
        .map(p -> p.partition()).collect(Collectors.toList());
    assertEquals(Arrays.asList(new Integer[] { 1, 2 }), filteredPartitions);
    Set<Integer> outputPartitionIds = new HashSet<>(Arrays.asList(1, 2));
    for (int i = 0; i < 10000; i++) {
      int partition = partitioner.partition(String.valueOf(i), partitions);
      assertTrue("Bad partition:" + partition, outputPartitionIds.contains(partition));
    }

    partitioner = new LocalityAwareRandomPartitioner("us-east-1c", 1000);
    partitioner.partition(String.valueOf(0), partitions);
    filteredPartitions = partitioner.getLocalPartitions().stream().map(p -> p.partition())
        .collect(Collectors.toList());
    assertEquals(4, filteredPartitions.size());
    assertEquals(Arrays.asList(new Integer[] { 0, 1, 3, 2 }), filteredPartitions);

    // validate update after 1.1 seconds
    Thread.sleep(1100);
    partitions = Arrays.asList(
        new PartitionInfo("topix", 0, new Node(0, "0", 9092, "us-east-1c"), null, null),
        new PartitionInfo("topix", 1, new Node(1, "0", 9092, "us-east-1e"), null, null),
        new PartitionInfo("topix", 3, new Node(2, "0", 9092, "us-east-1c"), null, null),
        new PartitionInfo("topix", 2, new Node(3, "0", 9092, "us-east-1d"), null, null));

    outputPartitionIds = new HashSet<>(Arrays.asList(0, 3));
    for (int i = 0; i < 10000; i++) {
      int partition = partitioner.partition(String.valueOf(i), partitions);
      assertTrue("Bad partition:" + partition, outputPartitionIds.contains(partition));
    }

    // validate update after 1.1 seconds
    Thread.sleep(1100);
    partitions = Arrays.asList(
        new PartitionInfo("topix", 0, new Node(0, "0", 9092, "us-east-1d"), null, null),
        new PartitionInfo("topix", 1, new Node(1, "0", 9092, "us-east-1e"), null, null),
        new PartitionInfo("topix", 3, new Node(2, "0", 9092, "us-east-1c"), null, null),
        new PartitionInfo("topix", 2, new Node(3, "0", 9092, "us-east-1d"), null, null));

    outputPartitionIds = new HashSet<>(Arrays.asList(3));
    for (int i = 0; i < 10000; i++) {
      int partition = partitioner.partition(String.valueOf(i), partitions);
      assertTrue("Bad partition:" + partition, outputPartitionIds.contains(partition));
    }
  }

  @Test
  public void testTimeToRefresh() {
    LocalityAwareRandomPartitioner partitioner = new LocalityAwareRandomPartitioner();
    
    partitioner.setNextRefreshTime(System.currentTimeMillis() - 10_000);
    partitioner.updateNextRefreshTime();
    assertFalse(partitioner.isTimeToRefresh());
    partitioner.setNextRefreshTime(System.currentTimeMillis() - 10_001);
    assertTrue(partitioner.isTimeToRefresh());

    partitioner = new LocalityAwareRandomPartitioner(null, 5000);
    partitioner.setNextRefreshTime(System.currentTimeMillis() - 5_000);
    partitioner.updateNextRefreshTime();
    assertFalse(partitioner.isTimeToRefresh());
    partitioner.setNextRefreshTime(System.currentTimeMillis() - 5_001);
    assertTrue(partitioner.isTimeToRefresh());
  }
  
}