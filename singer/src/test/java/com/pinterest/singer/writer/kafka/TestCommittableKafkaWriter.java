/**
 * Copyright 2020 Pinterest, Inc.
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
package com.pinterest.singer.writer.kafka;

import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.shade.org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.pinterest.singer.SingerTestBase;
import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerLog;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.writer.Crc32ByteArrayPartitioner;
import com.pinterest.singer.writer.KafkaMessagePartitioner;
import com.pinterest.singer.writer.KafkaProducerManager;

@RunWith(MockitoJUnitRunner.class)
public class TestCommittableKafkaWriter extends SingerTestBase {

  private static final int NUM_EVENTS = 1000;

  private static final int NUM_KEYS = 20;

  @Mock
  KafkaProducer<byte[], byte[]> producer;

  @Test
  public void testWriteLogMessagesWithCrcPartitioning() throws Exception {
    KafkaMessagePartitioner partitioner = new Crc32ByteArrayPartitioner();
    KafkaProducerConfig config = new KafkaProducerConfig();
    SingerSettings.setSingerConfig(new SingerConfig());
    KafkaProducerManager.injectTestProducer(config, producer);
    // default value for skip noleader partition is false
    CommittableKafkaWriter writer = new CommittableKafkaWriter(config, partitioner, "topicx", false,
        Executors.newCachedThreadPool());

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

    // message with same key will be put together in the same bucket (same
    // partition);
    List<String> keys = IntStream.range(0, NUM_KEYS).mapToObj(i -> "key" + i)
        .collect(Collectors.toList());
    Map<Integer, List<LogMessage>> msgPartitionMap = new HashMap<>();
    Map<Integer, List<ProducerRecord<byte[], byte[]>>> recordPartitionMap = new HashMap<>();
    Map<Integer, List<RecordMetadata>> metadataPartitionMap = new HashMap<>();
    HashFunction crc32 = Hashing.crc32();
    List<LogMessage> logMessages = new ArrayList<>();
    for (int i = 0; i < NUM_KEYS; i++) {
      for (int j = 0; j < NUM_EVENTS / NUM_KEYS; j++) {
        LogMessage logMessage = new LogMessage();
        logMessage.setKey(keys.get(i).getBytes());
        int messageId = logMessages.size();
        ByteBuffer buf = ByteBuffer.allocate(4).putInt(messageId);
        buf.flip();
        logMessage.setMessage(buf);
        logMessages.add(logMessage);
        int partitionId = Math
            .abs(crc32.hashBytes(logMessage.getKey()).asInt() % partitions.size());
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>("topicx",
            partitionId, logMessage.getKey(), logMessage.getMessage());
        RecordMetadata recordMetadata = new RecordMetadata(
            new TopicPartition(record.topic(), record.partition()), messageId, 0, 0, 0L,
            record.key().length, record.value().length);
        when(producer.send(record)).thenReturn(ConcurrentUtils.constantFuture(recordMetadata));

        if (msgPartitionMap.containsKey(partitionId)) {
          msgPartitionMap.get(partitionId).add(logMessage);
          recordPartitionMap.get(partitionId).add(record);
          metadataPartitionMap.get(partitionId).add(recordMetadata);
        } else {
          msgPartitionMap.put(partitionId, new ArrayList<>());
          recordPartitionMap.put(partitionId, new ArrayList<>());
          metadataPartitionMap.put(partitionId, new ArrayList<>());
          msgPartitionMap.get(partitionId).add(logMessage);
          recordPartitionMap.get(partitionId).add(record);
          metadataPartitionMap.get(partitionId).add(recordMetadata);
        }
      }
    }

    writer.startCommit();

    for (LogMessage msg : logMessages) {
      writer.writeLogMessageToCommit(msg);
    }
    Map<Integer, KafkaWritingTaskFuture> commitableBuckets = writer.getCommitableBuckets();
    for (int partitionId = 0; partitionId < partitions.size(); partitionId++) {
      KafkaWritingTaskFuture kafkaWritingTaskFuture = commitableBuckets.get(partitionId);
      List<Future<RecordMetadata>> recordMetadataList = kafkaWritingTaskFuture
          .getRecordMetadataList();
      if (recordMetadataList.size() == 0) {
        continue;
      }
      int partition = kafkaWritingTaskFuture.getPartitionInfo().partition();

      // verify the message order is what is expected by calling messageCollation()
      List<ProducerRecord<byte[], byte[]>> expectedRecords = recordPartitionMap.get(partitionId);
      assertEquals(expectedRecords.size(), recordMetadataList.size());
      for (int j = 0; j < recordMetadataList.size(); j++) {
        RecordMetadata md = recordMetadataList.get(j).get();
        ProducerRecord<byte[], byte[]> producerRecord = expectedRecords.get(j);
        // validate that the message id expected is correct partition
        assertEquals(partition, (int) producerRecord.partition());
        assertEquals(ByteBuffer.wrap(producerRecord.value()).getInt(), md.offset());
      }

    }

    writer.endCommit(logMessages.size());
    // validate if writes are throwing any error
    writer.close();
  }

  @Test
  public void testWriterWithHeadersInjectorEnabledWithWrongClass() throws Exception {
    SingerLog singerLog = new SingerLog(createSingerLogConfig("test", "/a/b/c"));
    LogStream logStream = new LogStream(singerLog, "test.tmp");
    KafkaMessagePartitioner partitioner = new Crc32ByteArrayPartitioner();
    KafkaProducerConfig config = new KafkaProducerConfig();
    SingerSettings.setSingerConfig(new SingerConfig());
    KafkaProducerManager.injectTestProducer(config, producer);
    // set wrong headerInjectorClass name, headerInjector will be null if
    // enableHeaderInjector is true
    logStream.getSingerLog().getSingerLogConfig().setHeadersInjectorClass("com.pinterest.x.y.z");
    @SuppressWarnings("resource")
    CommittableKafkaWriter writer = new CommittableKafkaWriter(logStream, config, partitioner,
        "topicx", false, Executors.newCachedThreadPool(), true);
    assertNull(writer.getHeadersInjector());
  }
}