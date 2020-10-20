package com.pinterest.singer.writer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.CRC32;

import com.google.common.primitives.Longs;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.pulsar.shade.org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.apache.thrift.TDeserializer;
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
import com.pinterest.singer.loggingaudit.client.AuditHeadersGenerator;
import com.pinterest.singer.loggingaudit.thrift.AuditDemoLog1Message;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditHeaders;
import com.pinterest.singer.loggingaudit.thrift.configuration.AuditConfig;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.utils.PartitionComparator;

@RunWith(MockitoJUnitRunner.class)
public class TestKafkaWriter extends SingerTestBase {

  private static final int NUM_EVENTS = 1000;

  private static final int NUM_KEYS = 20;

  private static final String LOGGING_AUDIT_HEADER_KEY = "loggingAuditHeaders";
  private static final String CHECKSUM_HEADER_KEY = "messageCRC";
  @Mock
  KafkaProducer<byte[], byte[]> producer;


  @Test
  public void testWriteLogMessagesWithCrcPartitioning() throws Exception {
    KafkaMessagePartitioner partitioner = new Crc32ByteArrayPartitioner();
    KafkaProducerConfig config = new KafkaProducerConfig();
    SingerSettings.setSingerConfig(new SingerConfig());
    KafkaProducerManager.injectTestProducer(config, producer);
    // default value for skip noleader partition is false
    KafkaWriter writer = new KafkaWriter(config, partitioner, "topicx", false, Executors.newCachedThreadPool());

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

    // message with same key will be put together in the same bucket (same partition);
    List<String> keys = IntStream.range(0, NUM_KEYS).mapToObj(i->"key"+i).collect(Collectors.toList());
    Map<Integer, List<LogMessage>> msgPartitionMap = new HashMap<>();
    Map<Integer, List<ProducerRecord<byte[], byte[]>>> recordPartitionMap = new HashMap<>();
    Map<Integer, List<RecordMetadata>> metadataPartitionMap = new HashMap<>();
    HashFunction crc32 = Hashing.crc32();
    List<LogMessage> logMessages = new ArrayList<>();
    for(int i = 0; i < NUM_KEYS; i++){
      for(int j = 0; j < NUM_EVENTS / NUM_KEYS; j++){
        LogMessage logMessage = new LogMessage();
        logMessage.setKey(keys.get(i).getBytes());
        logMessage.setMessage(ByteBuffer.allocate(100).put(String.valueOf(i).getBytes()));
        logMessages.add(logMessage);
        int partitionId = Math.abs(crc32.hashBytes(logMessage.getKey()).asInt() % partitions.size());
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>("topicx", partitionId, logMessage.getKey(), logMessage.getMessage());
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(  record.topic(),
            record.partition()), 0, 0, 0, 0L, record.key().length, record.value().length);
        when(producer.send(record)).thenReturn(ConcurrentUtils.constantFuture(recordMetadata));

        if (msgPartitionMap.containsKey(partitionId)){
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

    List<PartitionInfo> sortedPartitions = new ArrayList<>(partitions);
    Collections.sort(sortedPartitions, new PartitionComparator());

    Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfTrackedMessageMaps = new HashMap<>();
    Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfInvalidMessageMaps = new HashMap<>();
    Map<Integer, Integer> mapOfOriginalIndexWithinBucket = new HashMap<>();

    Map<Integer, List<ProducerRecord<byte[], byte[]>>> messageCollation = writer.messageCollation(
        partitions, "topicx", logMessages, mapOfTrackedMessageMaps, mapOfInvalidMessageMaps,
        mapOfOriginalIndexWithinBucket);

    for(int partitionId = 0; partitionId < messageCollation.keySet().size(); partitionId++) {
      if (messageCollation.get(partitionId).size() == 0) {
        continue;
      }
      List<ProducerRecord<byte[], byte[]>> writerOutput = messageCollation.get(partitionId);

      // verify the message order is what is expected by calling messageCollation()
      List<ProducerRecord<byte[], byte[]>> expectedRecords = recordPartitionMap.get(partitionId);
      assertEquals(expectedRecords.size(), writerOutput.size());
      for(int j = 0; j < writerOutput.size(); j++){
        assertEquals(expectedRecords.get(j), writerOutput.get(j));
      }

      // verify the content of LogMessage and the content of ProducerRecord match
      List<LogMessage> originalData = msgPartitionMap.get(partitionId);
      assertEquals(originalData.size(), writerOutput.size());
      for (int j = 0; j < writerOutput.size(); j++) {
        assertTrue(Arrays.equals(originalData.get(j).getKey(), writerOutput.get(j).key()));
        assertTrue(Arrays.equals(originalData.get(j).getMessage(), writerOutput.get(j).value()));
      }

      // verify the RecordMetadata that corresponds to record send to certain partitions are put
      // together into a list and the order of the RecordMetadata is same as the original message order
      List<RecordMetadata> expectedRecordMetadata = metadataPartitionMap.get(partitionId);
      KafkaWritingTaskResult kafkaWritingTaskResult  = writer.getClusterThreadPool().submit(new
          KafkaWritingTask(producer, writerOutput, 0, sortedPartitions)).get();
      assertEquals(expectedRecordMetadata.size(), kafkaWritingTaskResult.getRecordMetadataList().size());
      for(int j = 0; j < expectedRecordMetadata.size(); j++){
        assertEquals(expectedRecordMetadata.get(j), kafkaWritingTaskResult.getRecordMetadataList().get(j));
      }
    }
    // validate if writes are throwing any error
    writer.writeLogMessages(logMessages);
    writer.close();
  }

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

    Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfTrackedMessageMaps = new HashMap<>();
    Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfInvalidMessageMaps = new HashMap<>();
    Map<Integer, Integer> mapOfOriginalIndexWithinBucket = new HashMap<>();

    Map<Integer, List<ProducerRecord<byte[], byte[]>>> messageCollation = writer.messageCollation(
        partitions, "topicx", logMessages, mapOfTrackedMessageMaps, mapOfInvalidMessageMaps,
        mapOfOriginalIndexWithinBucket);

    // for (int i = 0; i < messageCollation.size(); i++) {
    for(Integer partitionId : messageCollation.keySet()){
      List<ProducerRecord<byte[], byte[]>> writerOutput = messageCollation.get(partitionId);
      List<LogMessage> originalData = msgPartitionMap.get(partitionId);
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
  public void testWrongHeadersInjectorClassWithHeadersInjectorEnabled() throws Exception {
    SingerLog singerLog = new SingerLog(createSingerLogConfig("test", "/a/b/c"));
    LogStream logStream = new LogStream(singerLog, "test.tmp");
    KafkaMessagePartitioner partitioner = new Crc32ByteArrayPartitioner();
    KafkaProducerConfig config = new KafkaProducerConfig();
    SingerSettings.setSingerConfig(new SingerConfig());
    KafkaProducerManager.injectTestProducer(config, producer);
    // set wrong headerInjectorClass name, headerInjector will be null if enableHeaderInjector is true
    logStream.getSingerLog().getSingerLogConfig().setHeadersInjectorClass("com.pinterest.x.y.z");
    @SuppressWarnings("resource")
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
    TSerializer serializer = new TSerializer();
    List<LogMessage> logMessages = new ArrayList<>();
    Set<LoggingAuditHeaders> trackedMessageSet = new HashSet<>();
    populateLogMessagesForLoggingAuditTests(logMessages, trackedMessageSet, true, 0.5);

    Set<LoggingAuditHeaders> invalidMessageSet = new HashSet<>();
    corruptLogMessages(logMessages, invalidMessageSet, 0.9);

    // assign each logMessage belongs to different partition
    for(LogMessage logMessage : logMessages) {
      int partitionId = Math.abs(crc32.hashBytes(logMessage.getKey()).asInt() % partitions.size());
      List<LogMessage> list = msgPartitionMap.get(partitionId);
      if (list == null) {
        list = new ArrayList<>();
        msgPartitionMap.put(partitionId, list);
      }
      list.add(logMessage);
    }

    Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfTrackedMessageMaps = new HashMap<>();
    Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfInvalidMessageMaps = new HashMap<>();
    Map<Integer, Integer> mapOfOriginalIndexWithinBucket = new HashMap<>();

    Map<Integer, List<ProducerRecord<byte[], byte[]>>> messageCollation = writer.messageCollation(
        partitions, "topicx", logMessages, mapOfTrackedMessageMaps, mapOfInvalidMessageMaps,
        mapOfOriginalIndexWithinBucket);

    // validate each bucket
    for (int i = 0; i < messageCollation.size(); i++) {
      List<ProducerRecord<byte[], byte[]>> writerOutput = messageCollation.get(i);
      List<LogMessage> originalData = msgPartitionMap.get(i);
      if (originalData == null) {
        assertEquals(0, writerOutput.size());
        continue;
      }
      // validate number of messages match for ith bucket (partition)
      assertEquals(originalData.size(), writerOutput.size());

      // validate each message in ith bucket
      for (int j = 0; j < writerOutput.size(); j++) {
        // validate key field
        assertTrue(Arrays.equals(originalData.get(j).getKey(), writerOutput.get(j).key()));
        // validate message field
        assertTrue(Arrays.equals(originalData.get(j).getMessage(), writerOutput.get(j).value()));

        // validate LoggingAuditHeaders field and checksum field
        boolean foundLoggingAuditHeaders = false;
        boolean foundMessageCRC = false;
        for(Header header : writerOutput.get(j).headers().toArray()){
          if (header.key().equals(LOGGING_AUDIT_HEADER_KEY)){
            LoggingAuditHeaders originalHeaders = originalData.get(j).getLoggingAuditHeaders();
            byte[] expected = serializer.serialize(originalHeaders);
            assertArrayEquals(expected, header.value());
            foundLoggingAuditHeaders = true;

            // validate certain messages are being tracked.
            if (originalHeaders.isTracked()){
              assertTrue(trackedMessageSet.contains(originalHeaders));
            }
          } else if (header.key().equals(CHECKSUM_HEADER_KEY)) {
            assertArrayEquals(Longs.toByteArray(originalData.get(j).getChecksum()), header.value());
            foundMessageCRC = true;
          }
        }
        assertTrue(foundLoggingAuditHeaders);
        assertTrue(foundMessageCRC);
      }
    }

    // validate tracked messages in mapOfTrackedMessageMaps
    for(Map<Integer, LoggingAuditHeaders> map : mapOfTrackedMessageMaps.values()){
      for(LoggingAuditHeaders h: map.values()){
        assertTrue(trackedMessageSet.contains(h));
        trackedMessageSet.remove(h);
      }
    }
    assertEquals(0, trackedMessageSet.size());

    // validate if writes are throwing any error
    writer.writeLogMessages(logMessages);
    writer.close();
  }

  @Test
  public void testWriterWithSkipCorruptedMessageEnabled() throws Exception {
    // corrupted messages are found and skipped
    KafkaWriter writer = setupWriterForCorruptedMessagesTests(true);

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

    List<LogMessage> logMessages = new ArrayList<>();
    Set<LoggingAuditHeaders> trackedMessageSet = new HashSet<>();
    populateLogMessagesForLoggingAuditTests(logMessages, trackedMessageSet, true, 0.65);

    Set<LoggingAuditHeaders> invalidMessageSet  = new HashSet<>();
    corruptLogMessages(logMessages, invalidMessageSet, 0.8);

    Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfTrackedMessageMaps = new HashMap<>();
    Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfInvalidMessageMaps = new HashMap<>();
    Map<Integer, Integer> mapOfOriginalIndexWithinBucket = new HashMap<>();
    Map<Integer, List<ProducerRecord<byte[], byte[]>>> messageCollation = writer.messageCollation(
        partitions, "topicx", logMessages, mapOfTrackedMessageMaps, mapOfInvalidMessageMaps,
        mapOfOriginalIndexWithinBucket);

    // validate messages in each bucket
    CRC32 checksumCalculator = new CRC32();
    TDeserializer deserializer = new TDeserializer();

    for(Integer partitionId : messageCollation.keySet()){
      List<ProducerRecord<byte[], byte[]>> writerOutput = messageCollation.get(partitionId);
      Map<Integer, LoggingAuditHeaders> trackedMessageMap = mapOfTrackedMessageMaps.get(partitionId);
      Map<Integer, LoggingAuditHeaders> invalidMessageMap = mapOfInvalidMessageMaps.get(partitionId);

      // validate LoggingAuditHeaders field and checksum field are injected
      // validate the value is uncorrupted as all corrupted messages have been skipped.
      for (int j = 0; j < writerOutput.size(); j++) {
        ProducerRecord<byte[], byte[]> record = writerOutput.get(j);
        boolean foundLoggingAuditHeaders = false;
        boolean foundMessageCRC = false;
        for(Header header :  record.headers().toArray()){
          if (header.key().equals(LOGGING_AUDIT_HEADER_KEY)){
            foundLoggingAuditHeaders = true;
            // validate certain messages are being tracked.
            LoggingAuditHeaders auditHeaders = new LoggingAuditHeaders();
            deserializer.deserialize(auditHeaders, header.value());
            if (auditHeaders.isTracked()){
              assertTrue(trackedMessageSet.contains(auditHeaders));
            }
          } else if (header.key().equals(CHECKSUM_HEADER_KEY)) {
            foundMessageCRC = true;
            // validate producerRecord's value is uncorrupted.
            checksumCalculator.reset();
            checksumCalculator.update(record.value());
            assertEquals(checksumCalculator.getValue(), Longs.fromByteArray(header.value()));
          }
        }
        assertTrue(foundLoggingAuditHeaders);
        assertTrue(foundMessageCRC);
      }

      // validate tracked messages are found during calling messageCollation method
      for(LoggingAuditHeaders h: trackedMessageMap.values()){
        assertTrue(trackedMessageSet.contains(h));
        trackedMessageSet.remove(h);
      }

      // validate invalid messages are found during calling messageCollation method
      for(LoggingAuditHeaders h: invalidMessageMap.values()){
        assertTrue(invalidMessageSet.contains(h));
        invalidMessageSet.remove(h);
      }

    }

    // validate all tracked messages are found
    assertEquals(0, trackedMessageSet.size());

    // validate all invalid messages are found
    assertEquals(0, invalidMessageSet.size());

    // validate if writes are throwing any error
    writer.writeLogMessages(logMessages);
    writer.close();
  }

  @Test
  public void testWriterWithSkipCorruptedMessageDisabled() throws Exception {
    // corrupted messages are found but not skipped
    KafkaWriter writer = setupWriterForCorruptedMessagesTests(false);

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

    List<LogMessage> logMessages = new ArrayList<>();
    Set<LoggingAuditHeaders> trackedMessageSet = new HashSet<>();
    populateLogMessagesForLoggingAuditTests(logMessages, trackedMessageSet, true, 0.65);

    Set<LoggingAuditHeaders> invalidMessageSet  = new HashSet<>();
    corruptLogMessages(logMessages, invalidMessageSet, 0.8);

    Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfTrackedMessageMaps = new HashMap<>();
    Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfInvalidMessageMaps = new HashMap<>();
    Map<Integer, Integer> mapOfOriginalIndexWithinBucket = new HashMap<>();
    Map<Integer, List<ProducerRecord<byte[], byte[]>>> messageCollation = writer.messageCollation(
        partitions, "topicx", logMessages, mapOfTrackedMessageMaps, mapOfInvalidMessageMaps,
        mapOfOriginalIndexWithinBucket);

    // validate messages in each bucket
    CRC32 checksumCalculator = new CRC32();
    TDeserializer deserializer = new TDeserializer();

    for(Integer partitionId : messageCollation.keySet()){
      List<ProducerRecord<byte[], byte[]>> writerOutput = messageCollation.get(partitionId);
      Map<Integer, LoggingAuditHeaders> trackedMessageMap = mapOfTrackedMessageMaps.get(partitionId);
      Map<Integer, LoggingAuditHeaders> invalidMessageMap = mapOfInvalidMessageMaps.get(partitionId);

      // validate LoggingAuditHeaders field and checksum field are injected
      for (int j = 0; j < writerOutput.size(); j++) {
        ProducerRecord<byte[], byte[]> record = writerOutput.get(j);
        boolean foundLoggingAuditHeaders = false;
        boolean foundMessageCRC = false;
        LoggingAuditHeaders auditHeaders = null;

        for(Header header :  record.headers().toArray()){
          if (header.key().equals(LOGGING_AUDIT_HEADER_KEY)){
            foundLoggingAuditHeaders = true;
            // validate certain messages are being tracked.
            auditHeaders = new LoggingAuditHeaders();
            deserializer.deserialize(auditHeaders, header.value());
            if (auditHeaders.isTracked()){
              assertTrue(trackedMessageSet.contains(auditHeaders));
            }
          } else if (header.key().equals(CHECKSUM_HEADER_KEY)) {
            foundMessageCRC = true;
            // validate certain producerRecord's value is corrupted
            checksumCalculator.reset();
            checksumCalculator.update(record.value());
            if (checksumCalculator.getValue() != Longs.fromByteArray(header.value())){
              assertTrue(invalidMessageSet.contains(auditHeaders));
            }
          }
        }
        assertTrue(foundLoggingAuditHeaders);
        assertTrue(foundMessageCRC);
      }

      // validate tracked messages are found during calling messageCollation method
      for(LoggingAuditHeaders h: trackedMessageMap.values()){
        assertTrue(trackedMessageSet.contains(h));
        trackedMessageSet.remove(h);
      }

      // validate invalid messages are found during calling messageCollation method
      for(LoggingAuditHeaders h: invalidMessageMap.values()){
        assertTrue(invalidMessageSet.contains(h));
        invalidMessageSet.remove(h);
      }

    }

    // validate all tracked messages are found
    assertEquals(0, trackedMessageSet.size());

    // validate all invalid messages are found
    assertEquals(0, invalidMessageSet.size());

    // validate if writes are throwing any error
    writer.writeLogMessages(logMessages);
    writer.close();
  }

  @SuppressWarnings("resource")
  @Test
  public void testCheckAndSetLoggingAuditHeadersForLogMessage() throws Exception{
    // prepare log messages
    List<LogMessage> logMessages = new ArrayList<>();
    Set<LoggingAuditHeaders> trackedMessageSet = new HashSet<>();
    List<LoggingAuditHeaders> originalHeaders = new ArrayList<>();
    populateLogMessagesForLoggingAuditTests(logMessages, trackedMessageSet, true, 0.65);
    for(LogMessage logMessage : logMessages){
      originalHeaders.add(logMessage.getLoggingAuditHeaders());
    }

    KafkaMessagePartitioner partitioner = new Crc32ByteArrayPartitioner();
    KafkaProducerConfig config = new KafkaProducerConfig();
    SingerSettings.setSingerConfig(new SingerConfig());
    KafkaProducerManager.injectTestProducer(config, producer);

    // case 1: enableLoggingAudit is false
    SingerLogConfig singerLogConfig1 = createSingerLogConfig("test", "/a/b/c");
    SingerLog singerLog1 = new SingerLog(singerLogConfig1);
    LogStream logStream1 = new LogStream(singerLog1, "test.tmp");

    KafkaWriter writer = new KafkaWriter(logStream1, config, partitioner, "topicx", false,
        Executors.newCachedThreadPool(), true);
    assertNull(writer.getAuditHeadersGenerator());
    assertNull(writer.getAuditConfig());
    assertFalse(writer.isEnableLoggingAudit());
    for(int i = 0; i < logMessages.size(); i++){
      writer.checkAndSetLoggingAuditHeadersForLogMessage(logMessages.get(i));
      assertEquals(originalHeaders.get(i), logMessages.get(i).getLoggingAuditHeaders());
    }

    // case 2: enableLoggingAudit is true but logging audit does not start at current stage.
    SingerLogConfig singerLogConfig2 = createSingerLogConfig("test", "/a/b/c");
    singerLogConfig2.setEnableLoggingAudit(true);
    singerLogConfig2.setAuditConfig(new AuditConfig().setStartAtCurrentStage(false)
        .setStopAtCurrentStage(false).setSamplingRate(1.0));

    SingerLog singerLog2 = new SingerLog(singerLogConfig2);
    LogStream logStream2 = new LogStream(singerLog2, "test.tmp");
    writer = new KafkaWriter(logStream2, config, partitioner, "topicx", false,
        Executors.newCachedThreadPool(), true);

    assertNotNull(writer.getAuditHeadersGenerator());
    assertNotNull(writer.getAuditConfig());
    assertTrue(writer.isEnableLoggingAudit());
    for(int i = 0; i < logMessages.size(); i++){
      writer.checkAndSetLoggingAuditHeadersForLogMessage(logMessages.get(i));
      assertEquals(originalHeaders.get(i), logMessages.get(i).getLoggingAuditHeaders());
    }

    // case 3: enableLoggingAudit is true and logging audit starts at current stage.
    SingerLogConfig singerLogConfig3 = createSingerLogConfig("test", "/a/b/c");
    singerLogConfig3.setEnableLoggingAudit(true);
    singerLogConfig3.setAuditConfig(new AuditConfig().setStartAtCurrentStage(true)
        .setStopAtCurrentStage(false).setSamplingRate(1.0));

    SingerLog singerLog3 = new SingerLog(singerLogConfig3);
    LogStream logStream3 = new LogStream(singerLog3, "test.tmp");
    writer = new KafkaWriter(logStream3, config, partitioner, "topicx", false,
        Executors.newCachedThreadPool(), true);

    // inject auditHeadersGenerator for header verification
    String hostName = "testHostName";
    String logName = "testLogName";
    writer.setAuditHeadersGenerator(new AuditHeadersGenerator(hostName, logName));
    assertNotNull(writer.getAuditHeadersGenerator());
    assertNotNull(writer.getAuditConfig());
    assertTrue(writer.isEnableLoggingAudit());
    for(int i = 0; i < logMessages.size(); i++) {
      writer.checkAndSetLoggingAuditHeadersForLogMessage(logMessages.get(i));
      assertNotEquals(originalHeaders.get(i), logMessages.get(i).getLoggingAuditHeaders());
      LoggingAuditHeaders headers = logMessages.get(i).getLoggingAuditHeaders();
      assertEquals(hostName, headers.getHost());
      assertEquals(logName, headers.getLogName());
      assertEquals(i, headers.getLogSeqNumInSession());
    }
    writer.close();
  }


  @Test
  public void testCheckMessageValidByChecksum() throws Exception {

    // prepare logMessages
    List<LogMessage> logMessages = new ArrayList<>();
    Set<LoggingAuditHeaders> trackedMessageSet = new HashSet<>();
    populateLogMessagesForLoggingAuditTests(logMessages, trackedMessageSet, true, 0.65);

    Set<LoggingAuditHeaders> invalidMessageSet  = new HashSet<>();
    corruptLogMessages(logMessages, invalidMessageSet, 0.8);

    // setup writer
    KafkaWriter writer = setupWriterForCorruptedMessagesTests(true);
    for(LogMessage logMessage : logMessages) {
      boolean valid =  writer.checkMessageValid(logMessage);
      if (!valid) {
        assertTrue(invalidMessageSet.contains(logMessage.getLoggingAuditHeaders()));
        invalidMessageSet.remove(logMessage.getLoggingAuditHeaders());
      }
    }
    assertEquals(0, invalidMessageSet.size());
  }

  public KafkaWritingTaskResult createResult(KafkaWritingTask worker, List<PartitionInfo> sortedPartitions){
    long start = System.currentTimeMillis();
    List<ProducerRecord<byte[], byte[]>> messages = worker.getMessages();
    List<RecordMetadata> recordMetadataList = new ArrayList<>();
    int bytes = 0;
    for(int i = 0; i < messages.size(); i++){
      int keySize = messages.get(i).key().length;
      int valSize = messages.get(i).value().length;
      bytes += keySize + valSize;
      int partition = messages.get(i).partition();
      String topic = sortedPartitions.get(partition).topic();
      TopicPartition topicPartition = new TopicPartition(topic,partition);
      recordMetadataList.add(new RecordMetadata(topicPartition, 0,0,0,0L, keySize, valSize));
    }
    long end = System.currentTimeMillis();
    KafkaWritingTaskResult  kafkaWritingTaskResult = new KafkaWritingTaskResult(true, bytes, (int)(end - start));
    kafkaWritingTaskResult.setPartition(messages.get(0).partition());
    kafkaWritingTaskResult.setRecordMetadataList(recordMetadataList);
    return kafkaWritingTaskResult;
  }

  /**
   * helper method that sets up KafkaWriter
   */
  private KafkaWriter setupWriterForCorruptedMessagesTests(boolean skipCorruptedMessages) {
    KafkaMessagePartitioner partitioner = new Crc32ByteArrayPartitioner();
    KafkaProducerConfig config = new KafkaProducerConfig();
    SingerSettings.setSingerConfig(new SingerConfig());
    KafkaProducerManager.injectTestProducer(config, producer);

    SingerLogConfig singerLogConfig = createSingerLogConfig("test", "/a/b/c");
    singerLogConfig.setEnableLoggingAudit(true);
    // enable deletion of corrupted messages

    AuditConfig auditConfig = new AuditConfig().setStartAtCurrentStage(false)
        .setStopAtCurrentStage(false).setSamplingRate(1.0)
        .setSkipCorruptedMessageAtCurrentStage(skipCorruptedMessages);


    singerLogConfig.setAuditConfig(auditConfig);

    SingerLog singerLog = new SingerLog(singerLogConfig);
    LogStream logStream = new LogStream(singerLog, "test.tmp");
    KafkaWriter writer = new KafkaWriter(logStream, config, partitioner, "topicx", false,
        Executors.newCachedThreadPool(), true);

    return writer;
  }


  /**
   * helper method that populates the logMessages for testWriterWithSkipCorruptedMessagesEnabled()
   * and testWriterWithSkipCorruptedMessagesDisabled()
   */
  private void populateLogMessagesForLoggingAuditTests(List<LogMessage> logMessages,
                                                       Set<LoggingAuditHeaders> trackedMessageSet,
                                                       boolean loggingAuditEnabled,
                                                       double auditRate) throws Exception {

    int payloadLen = 100;
    TSerializer serializer = new TSerializer();
    CRC32 checksumCalculator = new CRC32();
    int pid = new Random().nextInt();
    long session = System.currentTimeMillis();
    for (int i = 0; i < NUM_EVENTS; i++) {
      LogMessage logMessage = new LogMessage();
      logMessage.setKey(String.valueOf(i).getBytes());


      byte[] paylaod = RandomStringUtils.randomAlphabetic(payloadLen).getBytes();
      AuditDemoLog1Message messageObject = new AuditDemoLog1Message().setSeqId(i).setPayload(
          paylaod).setTimestamp(System.currentTimeMillis());
      byte[] message = serializer.serialize(messageObject);
      logMessage.setMessage(message);

      if (loggingAuditEnabled) {
        // set LoggingAuditHeaders for every message
        LoggingAuditHeaders headers = new LoggingAuditHeaders()
            .setHost("host-name")
            .setLogName("topicx")
            .setPid(pid)
            .setSession(session)
            .setLogSeqNumInSession(i);
        // track 20 percent of messages
        if (ThreadLocalRandom.current().nextDouble() < auditRate) {
          headers.setTracked(true);
          trackedMessageSet.add(headers);
        }
        logMessage.setLoggingAuditHeaders(headers);
        // set checksum for every message
        checksumCalculator.reset();
        checksumCalculator.update(logMessage.getMessage());
        logMessage.setChecksum(checksumCalculator.getValue());
      }
      logMessages.add(logMessage);
    }
  }

  /**
   * helper method that corrupt last byte of a given byte array
   */

  public void corruptByte(byte[] message){
    if (message.length > 0){
      // corrupt last byte
      message[message.length-1] = (byte)(~message[message.length-1]);
    }
  }

  /**
   * helper method that randomly corrupt certain LogMessage based on corruptionRate
   */
  private void corruptLogMessages(List<LogMessage> logMessages,
                                  Set<LoggingAuditHeaders> invalidMessageSet,
                                  double corruptionRate){
    CRC32 checksumCalculator = new CRC32();
    int total = logMessages.size();
    for(int i = 0; i < total; i++){
      LogMessage logMessage = logMessages.get(i);

      // randomly corrupt logMessage based on corruptionRate
      if (ThreadLocalRandom.current().nextDouble() < corruptionRate){
        // make sure checksum mismatches
        byte[] newMessage = Arrays.copyOf(logMessage.getMessage(), logMessage.getMessage().length);
        corruptByte(newMessage);
        checksumCalculator.reset();
        checksumCalculator.update(newMessage);
        if (checksumCalculator.getValue() != logMessage.getChecksum()){
          logMessage.setMessage(newMessage);
          invalidMessageSet.add(logMessage.getLoggingAuditHeaders());
        }
      }
    }
  }

}