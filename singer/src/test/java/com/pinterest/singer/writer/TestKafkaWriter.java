package com.pinterest.singer.writer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.pinterest.singer.thrift.LogMessage;

@RunWith(MockitoJUnitRunner.class)
public class TestKafkaWriter {

  private static final int NUM_EVENTS = 1000;
  @Mock
  KafkaProducer<byte[], byte[]> producer;

  @Test
  public void testPartitioningParityCRC() throws Exception {
    KafkaMessagePartitioner partitioner = new Crc32ByteArrayPartitioner();
    // default value for skip noleader partition is false
    KafkaWriter writer = new KafkaWriter(partitioner, false);

    List<PartitionInfo> partitions = ImmutableList.copyOf(Arrays.asList(
        new PartitionInfo("topicx", 1, new Node(2, "broker2", 9092, "us-east-1b"), null, null),
        new PartitionInfo("topicx", 0, new Node(1, "broker1", 9092, "us-east-1a"), null, null),
        new PartitionInfo("topicx", 2, new Node(3, "broker3", 9092, "us-east-1c"), null, null),
        new PartitionInfo("topicx", 6, new Node(2, "broker2", 9092, "us-east-1b"), null, null),
        new PartitionInfo("topicx", 3, new Node(4, "broker4", 9092, "us-east-1a"), null, null),
        new PartitionInfo("topicx", 5, new Node(1, "broker1", 9092, "us-east-1a"), null, null),
        new PartitionInfo("topicx", 7, new Node(3, "broker3", 9092, "us-east-1c"), null, null),
        new PartitionInfo("topicx", 4, new Node(5, "broker5", 9092, "us-east-1b"), null, null),
        new PartitionInfo("topicx", 8, new Node(4, "broker4", 9092, "us-east-1a"), null, null),
        new PartitionInfo("topicx", 9, new Node(5, "broker5", 9092, "us-east-1b"), null, null),
        new PartitionInfo("topicx", 10, new Node(1, "broker1", 9092, "us-east-1a"), null, null)));
    when(producer.partitionsFor("topicx")).thenReturn(partitions);

    Map<Integer, List<LogMessage>> msgPartitionMap = new HashMap<>();
    HashFunction crc32 = Hashing.crc32();
    List<LogMessage> logMessages = new ArrayList<>();
    for (int i = 0; i < NUM_EVENTS; i++) {
      LogMessage logMessage = new LogMessage();
      logMessage.setKey(ByteBuffer.allocate(100).put(String.valueOf(i).getBytes()));
      logMessage.setMessage(ByteBuffer.allocate(100).put(String.valueOf(i).getBytes()));
      logMessages.add(logMessage);

      int partitionId = Math.abs(crc32.hashBytes(logMessage.getKey()).asInt() % partitions.size());
      List<LogMessage> list = msgPartitionMap.get(partitionId);
      if (list == null) {
        list = new ArrayList<>();
        msgPartitionMap.put(partitionId, list);
      }
      list.add(logMessage);
    }

    List<List<ProducerRecord<byte[], byte[]>>> messageCollation = writer.messageCollation(producer,
        "topicx", logMessages);
    for (int i = 0; i < messageCollation.size(); i++) {
      List<ProducerRecord<byte[], byte[]>> writerOutput = messageCollation.get(i);
      List<LogMessage> originalData = msgPartitionMap.get(i);
      if (originalData == null) {
        assertEquals(0, writerOutput.size());
        continue;
      }
      assertEquals(originalData.size(), writerOutput.size());
      for (int j = 0; j < writerOutput.size(); j++) {
        assertTrue(Arrays.equals(originalData.get(j).getKey(), writerOutput.get(j).key()));
      }
    }
    writer.close();
  }

}