package com.pinterest.singer.writer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
import com.pinterest.singer.loggingaudit.client.AuditHeadersGenerator;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditHeaders;
import com.pinterest.singer.loggingaudit.thrift.configuration.AuditConfig;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.utils.PartitionComparator;
import com.pinterest.singer.writer.headersinjectors.LoggingAuditHeadersInjector;

@RunWith(MockitoJUnitRunner.class)
public class TestKafkaWriter extends SingerTestBase {

  private static final int NUM_EVENTS = 1000;

  private static final int NUM_KEYS = 20;

  @Mock
  KafkaProducer<byte[], byte[]> producer;

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

    Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfHeadersMap = new HashMap<>();
    Map<Integer, List<ProducerRecord<byte[], byte[]>>> messageCollation = writer.messageCollation(partitions, "topicx", logMessages, mapOfHeadersMap);

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

    Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfHeadersMap = new HashMap<>();
    Map<Integer, List<ProducerRecord<byte[], byte[]>>> messageCollation = writer.messageCollation(partitions, "topicx", logMessages, mapOfHeadersMap);

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
          .setLogName("topicx")
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

    Map<Integer, Map<Integer, LoggingAuditHeaders>> mapOfHeadersMap = new HashMap<>();
    Map<Integer, List<ProducerRecord<byte[], byte[]>>> messageCollation = writer
        .messageCollation(partitions, "topicx", logMessages, mapOfHeadersMap);
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

  @Test
  public void testCheckAndSetLoggingAuditHeadersForLogMessage() throws Exception{
    // prepare log messages
    List<LogMessage> logMessages = new ArrayList<>();
    List<LoggingAuditHeaders> originalHeaders = new ArrayList<>();
    int pid = new Random().nextInt();
    long session = System.currentTimeMillis();
    TSerializer serializer = new TSerializer();
    for (int i = 0; i < NUM_EVENTS; i++) {
      LogMessage logMessage = new LogMessage();
      logMessage.setKey(ByteBuffer.allocate(100).put(String.valueOf(i).getBytes()));
      logMessage.setMessage(ByteBuffer.allocate(100).put(String.valueOf(i).getBytes()));
      LoggingAuditHeaders headers = new LoggingAuditHeaders()
          .setHost("host-name")
          .setLogName("topicx")
          .setPid(pid)
          .setSession(session)
          .setLogSeqNumInSession(i);
      originalHeaders.add(headers);
      logMessage.setLoggingAuditHeaders(headers);
      logMessages.add(logMessage);
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
}