package com.pinterest.singer.writer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.pulsar.shade.org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.thrift.configuration.SingerConfig;

@RunWith(MockitoJUnitRunner.class)
public class TestKafkaWriter {

  private static final Logger LOG = LoggerFactory.getLogger(TestKafkaWriter.class);

  private static final int NUM_EVENTS = 1000;
  @Mock
  KafkaProducer<byte[], byte[]> producer;

  @Test
  public void testPartitioningParityCRC() throws Exception {
    KafkaMessagePartitioner partitioner = new Crc32ByteArrayPartitioner();
    KafkaProducerConfig config = new KafkaProducerConfig();
    SingerSettings.setSingerConfig(new SingerConfig());
    KafkaProducerManager.injectTestProducer(config, producer);
    // default value for skip noleader partition is false
    KafkaWriter writer = new KafkaWriter(config, partitioner, "topicx", false,
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
    when(producer.send(any())).thenReturn(ConcurrentUtils.constantFuture(
        new RecordMetadata(new TopicPartition("topicx", 0), 0L, 0L, 0L, 0L, 0, 0)));
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

    LOG.info("verify the old method (messageCollation) and refactor methods (logMessageCollation "
        + "and createProducerRecordFromLogMessage) can reorder LogMessages with the same "
        + "partitionId together ");
    List<List<LogMessage>>  messageBuckets =
        writer.logMessagesCollation(partitions, "topicx", logMessages);
    List<List<ProducerRecord<byte[], byte[]>>> messageCollation =
        writer.createProducerRecordFromLogMessage(messageBuckets, "topicx");

    List<List<ProducerRecord<byte[], byte[]>>> messageCollationOld =
        writer.messageCollation(partitions, "topicx", logMessages);
    assertEquals(messageCollationOld.size(), messageCollation.size());
    for(int i = 0; i < messageCollation.size(); i++){
      assertEquals(messageCollation.get(i).size(), messageCollationOld.get(i).size());
      for(int j = 0; j < messageCollation.get(i).size(); j++){
        assertTrue(Arrays.equals(messageCollation.get(i).get(j).key(),
            messageCollationOld.get(i).get(j).key()));
        assertTrue(Arrays.equals(messageCollation.get(i).get(j).value(),
            messageCollationOld.get(i).get(j).value()));
      }
    }

    LOG.info("verify the the correct headers are added when generating ProducerRecord");
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
        Header[] headers = writerOutput.get(j).headers().toArray();
        assertEquals("hostname", headers[0].key());
        assertEquals("locality", headers[1].key());
        assertEquals(HeaderProvider.HOSTNAME, headers[0].value());
        assertEquals(HeaderProvider.LOCALITY, headers[1].value());
      }
    }

    // validate if writes are throwing any error
    writer.writeLogMessages(logMessages);
    writer.close();
  }

}