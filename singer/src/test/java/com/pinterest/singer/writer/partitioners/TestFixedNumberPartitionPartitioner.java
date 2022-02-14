package com.pinterest.singer.writer.partitioners;

import com.pinterest.singer.writer.KafkaMessagePartitioner;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestFixedNumberPartitionPartitioner {
  @Test
  public void testFixedPartitionsPartitioner() {
    KafkaMessagePartitioner partitioner = new FixedPartitionsPartitioner();
    Map<String, String> configs = new HashMap<>();
    configs.put(FixedPartitionsPartitioner.PARTITIONER_PARTITION_COUNT_KEY, "2");
    partitioner.configure(configs);

    List<PartitionInfo> partitions = new ArrayList<>(Arrays.asList(
        new PartitionInfo("topix", 0, new Node(0, "0", 9092, "us-east-1a"), null, null),
        new PartitionInfo("topix", 1, new Node(0, "0", 9092, "us-east-1c"), null, null),
        new PartitionInfo("topix", 2, new Node(0, "0", 9092, "us-east-1d"), null, null),
        new PartitionInfo("topix", 4, new Node(0, "0", 9092, "us-east-1d"), null, null),
        new PartitionInfo("topix", 3, new Node(0, "0", 9092, "us-east-1e"), null, null)));

    Set<Integer> partitionSet = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      int p = partitioner.partition(null, partitions);
      partitionSet.add(p);
    }
    Assert.assertEquals("Should have only 2 partitions", 2, partitionSet.size());

    for (int i = 5; i < 500; i++) {
      partitions.add(new PartitionInfo("topix", 0, new Node(i, "0", 9092, "us-east-1a"), null, null));
    }

    Set<Integer> newPartitionSet = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      int p = partitioner.partition(null, partitions);
      newPartitionSet.add(p);
    }
    Assert.assertEquals("Should have only 2 partitions", 2, newPartitionSet.size());
    Assert.assertNotEquals("The partitions should be different between two runs (very low chance that the two are identical)", partitionSet, newPartitionSet);

  }

  @Test
  public void testFixedPartitionsPartitionerNumPartitionSmallerThanConfig() {
    KafkaMessagePartitioner partitioner = new FixedPartitionsPartitioner();
    Map<String, String> configs = new HashMap<>();
    configs.put(FixedPartitionsPartitioner.PARTITIONER_PARTITION_COUNT_KEY, "5");
    partitioner.configure(configs);

    List<PartitionInfo> partitions = new ArrayList<>(Arrays.asList(
        new PartitionInfo("topix", 0, new Node(0, "0", 9092, "us-east-1a"), null, null),
        new PartitionInfo("topix", 1, new Node(0, "0", 9092, "us-east-1c"), null, null),
        new PartitionInfo("topix", 2, new Node(0, "0", 9092, "us-east-1d"), null, null)));

    Set<Integer> partitionSet = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      int p = partitioner.partition(null, partitions);
      partitionSet.add(p);
    }
    Assert.assertEquals("Should have only 3 partitions", 3, partitionSet.size());
  }

}
