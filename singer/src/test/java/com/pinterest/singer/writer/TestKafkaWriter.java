package com.pinterest.singer.writer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.pulsar.shade.org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.apache.thrift.TSerializer;
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
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditHeaders;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.writer.headersinjectors.LoggingAuditHeadersInjector;

@RunWith(MockitoJUnitRunner.class)
public class TestKafkaWriter extends SingerTestBase {

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

    List<List<ProducerRecord<byte[], byte[]>>> messageCollation = writer
        .messageCollation(partitions, "topicx", logMessages);
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

    // validate if writes are throwing any error
    writer.writeLogMessages(logMessages);
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
    // set wrong headerInjectorClass name, headerInjector will be null if enableHeaderInjector is true
    logStream.getSingerLog().getSingerLogConfig().setHeadersInjectorClass("com.pinterest.x.y.z");
    KafkaWriter writer = new KafkaWriter(logStream, config, partitioner, "topicx", false,
        Executors.newCachedThreadPool(), true);
    assertNull(writer.getHeadersInjector());
  }

    @Test
  public void testWriterWithHeadersInjectorEnabled() throws Exception {
    SingerLog singerLog = new SingerLog(createSingerLogConfig("test", "/a/b/c"));
    LogStream logStream = new LogStream(singerLog, "test.tmp");
    KafkaMessagePartitioner partitioner = new Crc32ByteArrayPartitioner();
    KafkaProducerConfig config = new KafkaProducerConfig();
    SingerSettings.setSingerConfig(new SingerConfig());
    KafkaProducerManager.injectTestProducer(config, producer);
    KafkaWriter writer = new KafkaWriter(logStream, config, partitioner, "topicx", false,
        Executors.newCachedThreadPool(), true);
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
    int pid = new Random().nextInt();
    long session = System.currentTimeMillis();
    TSerializer serializer = new TSerializer();

    for (int i = 0; i < NUM_EVENTS; i++) {
      LogMessage logMessage = new LogMessage();
      logMessage.setKey(ByteBuffer.allocate(100).put(String.valueOf(i).getBytes()));
      logMessage.setMessage(ByteBuffer.allocate(100).put(String.valueOf(i).getBytes()));
      LoggingAuditHeaders headers = new LoggingAuditHeaders()
          .setHost("host-name")
          .setTopic("topicx")
          .setPid(pid)
          .setSession(session)
          .setLogSeqNumInSession(i);
      logMessage.setLoggingAuditHeaders(headers);
      logMessages.add(logMessage);

      int partitionId = Math.abs(crc32.hashBytes(logMessage.getKey()).asInt() % partitions.size());
      List<LogMessage> list = msgPartitionMap.get(partitionId);
      if (list == null) {
        list = new ArrayList<>();
        msgPartitionMap.put(partitionId, list);
      }
      list.add(logMessage);
    }

    List<List<ProducerRecord<byte[], byte[]>>> messageCollation = writer
        .messageCollation(partitions, "topicx", logMessages);
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
        boolean foundLoggingAuditHeaders = false;
        for(Header header : writerOutput.get(j).headers().toArray()){
          if (header.key().equals(LoggingAuditHeadersInjector.getHeaderKey())){
            byte[] expected = serializer.serialize(originalData.get(j).getLoggingAuditHeaders());
            assertArrayEquals(expected, header.value());
            foundLoggingAuditHeaders = true;
            break;
          }
        }
        assertTrue(foundLoggingAuditHeaders);
      }
    }
    // validate if writes are throwing any error
    writer.writeLogMessages(logMessages);
    writer.close();
  }
}