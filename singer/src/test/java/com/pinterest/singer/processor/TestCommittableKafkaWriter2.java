package com.pinterest.singer.processor;

import com.google.common.collect.Lists;
import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.LogStreamReader;
import com.pinterest.singer.common.SingerLog;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.common.errors.LogStreamProcessorException;
import com.pinterest.singer.common.errors.LogStreamWriterException;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.monitor.LogStreamManager;
import com.pinterest.singer.reader.DefaultLogStreamReader;
import com.pinterest.singer.reader.ThriftLogFileReaderFactory;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.LogMessageAndPosition;
import com.pinterest.singer.thrift.LogPosition;
import com.pinterest.singer.thrift.configuration.FileNameMatchMode;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.thrift.configuration.SingerRestartConfig;
import com.pinterest.singer.thrift.configuration.ThriftReaderConfig;
import com.pinterest.singer.utils.SimpleThriftLogger;
import com.pinterest.singer.utils.WatermarkUtils;
import com.pinterest.singer.writer.Crc32ByteArrayPartitioner;
import com.pinterest.singer.writer.KafkaWriter;
import com.pinterest.singer.writer.kafka.CommittableKafkaWriter;
import com.pinterest.singer.writer.kafka.KafkaWritingTaskFuture;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import org.apache.commons.io.FilenameUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestCommittableKafkaWriter2 {

    @ClassRule
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private static String brokerEndpoint;

    private Random random;

    @Before
    public void setup() throws Exception {
        int port = sharedKafkaTestResource.getKafkaTestUtils().describeClusterNodes().iterator().next().port();
        brokerEndpoint = "localhost:" + port;
        random = new Random();
        sharedKafkaTestResource.getKafkaTestUtils().createTopic("topicx", 1, (short) 1);
        Thread.sleep(3000);
    }

    private class BuggyTestCommittableKafkaWriterWithFlush extends CommittableKafkaWriter {

        public BuggyTestCommittableKafkaWriterWithFlush(LogStream logStream, KafkaProducerConfig producerConfig,
                                                        String topic,
                                                        boolean skipNoLeaderPartitions, boolean auditingEnabled,
                                                        String auditTopic,
                                                        String partitionerClassName, int writeTimeoutInSeconds,
                                                        boolean enableHeadersInjector) throws Exception {
            super(logStream, producerConfig, topic, skipNoLeaderPartitions, auditingEnabled, auditTopic,
                  partitionerClassName,
                  writeTimeoutInSeconds, enableHeadersInjector);
        }

        // flush first
        @Override
        public void endCommit(int numLogMessages, boolean isDraining) throws LogStreamWriterException {
            committableProducer.flush();
            super.endCommit(numLogMessages, isDraining);
        }
    }

    private class TestCommittableKafkaWriter extends CommittableKafkaWriter {

        List<CompletableFuture<RecordMetadata>> recordMetadataList = new ArrayList<>();
        List<ProducerRecord<byte[], byte[]>> producerRecordsList = new ArrayList<>();

        boolean timeoutExceptionReached = false;

        public TestCommittableKafkaWriter(LogStream logStream, KafkaProducerConfig producerConfig, String topic,
                                          boolean skipNoLeaderPartitions, boolean auditingEnabled, String auditTopic,
                                          String partitionerClassName, int writeTimeoutInSeconds, boolean enableHeadersInjector) throws Exception {
            super(logStream, producerConfig, topic, skipNoLeaderPartitions, auditingEnabled, auditTopic,
                  partitionerClassName,
                  writeTimeoutInSeconds, enableHeadersInjector);
        }

        // copied and pasted from parent class but adding list to keep track of producer records
        @Override
        public void writeLogMessageToCommit(LogMessageAndPosition message, boolean isDraining) throws LogStreamWriterException {
            LogMessage msg = message.getLogMessage();
            ProducerRecord<byte[], byte[]> keyedMessage;
            byte[] key = null;
            if (msg.isSetKey()) {
                key = msg.getKey();
            }
            int partitionId = partitioner.partition(key, committableValidPartitions);
            if (skipNoLeaderPartitions) {
                partitionId = committableValidPartitions.get(partitionId).partition();
            }
            keyedMessage = new ProducerRecord<>(topic, partitionId, key, msg.getMessage());
            Headers headers = keyedMessage.headers();
            addStandardHeaders(message, headers);
            checkAndSetLoggingAuditHeadersForLogMessage(msg);
            committableMapOfOriginalIndexWithinBucket.put(partitionId, 1 + committableMapOfOriginalIndexWithinBucket.get(partitionId));

            // add to list
            producerRecordsList.add(keyedMessage);

            if (msg.getLoggingAuditHeaders() != null) {
                // check if the message should be skipped
                if (checkMessageValidAndInjectHeaders(msg, headers, committableMapOfOriginalIndexWithinBucket.get(partitionId), partitionId,
                                                      committableMapOfTrackedMessageMaps, committableMapOfInvalidMessageMaps)) {
                    return;
                }
            }

            KafkaWritingTaskFuture kafkaWritingTaskFutureResult = committableBuckets.get(partitionId);
            List<CompletableFuture<RecordMetadata>> recordMetadataList = kafkaWritingTaskFutureResult
                    .getRecordMetadataList();

            if (recordMetadataList.isEmpty()) {
                kafkaWritingTaskFutureResult.setFirstProduceTimestamp(System.currentTimeMillis());
            }

            CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
            committableProducer.send(keyedMessage, (recordMetadata, exception) -> {
                if (exception != null) {
                    future.completeExceptionally(exception);
                } else {
                    future.complete(recordMetadata);
                }
            });
            recordMetadataList.add(future);
        }

        // copied and pasted from parent class but added list to track recordMetadataFutures
        @Override
        public void endCommit(int numLogMessages, boolean isDraining) throws LogStreamWriterException {

            List<CompletableFuture<Integer>> bucketFutures = new ArrayList<>();
            for(KafkaWritingTaskFuture f : committableBuckets.values()) {
                List<CompletableFuture<RecordMetadata>> futureList = f.getRecordMetadataList();
                if (futureList.isEmpty()) {
                    continue;
                }
                long start = f.getFirstProduceTimestamp();
                int leaderNode = f.getPartitionInfo().leader().id();
                int size = futureList.size();
                OpenTsdbMetricConverter.addMetric(SingerMetrics.WRITER_BATCH_SIZE, size, "topic=" + topic,
                                                  "host=" + KafkaWriter.HOSTNAME);

                // resolves with the latency of that bucket
                CompletableFuture<Integer> bucketFuture = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
                   .handleAsync((v, t) -> {
                       if (t != null) {
                           handleBucketException(leaderNode, size, isDraining, t);
                           if (t instanceof RuntimeException) {
                               throw (RuntimeException) t;
                           } else {
                               throw new RuntimeException(t);
                           }
                       }
                       int kafkaLatency = (int) (System.currentTimeMillis() - start);
                       // we shouldn't have latency greater than 2B milliseconds so it should be okay
                       // to downcast to integer
                       OpenTsdbMetricConverter.incrGranular(SingerMetrics.BROKER_WRITE_SUCCESS, 1,
                                                            "broker=" + leaderNode, "drain=" + isDraining);
                       OpenTsdbMetricConverter.addGranularMetric(SingerMetrics.BROKER_WRITE_LATENCY,
                                                                 kafkaLatency, "broker=" + leaderNode, "drain=" + isDraining);
                       return kafkaLatency;
                   });
                bucketFutures.add(bucketFuture);
            }
            CompletableFuture<Void> batchFuture = CompletableFuture.allOf(bucketFutures.toArray(new CompletableFuture[0]));

            // Set a timeout task that will cause the batch future to fail after writeTimeoutInSeconds
            CompletableFuture<Void> timerFuture = new CompletableFuture<>();
            Future<?> timerTask = executionTimer.schedule(() -> {
                timerFuture.completeExceptionally(new TimeoutException("Kafka batch write timed out after " + writeTimeoutInSeconds + " seconds"));
            }, writeTimeoutInSeconds, TimeUnit.SECONDS);

            CompletableFuture<Void> writerFuture = batchFuture
                    .applyToEitherAsync(timerFuture, Function.identity())
                    .whenComplete(
                            (v, t) -> {
                                if (t != null) {
                                    // add this logic here to verify timeout exception is reached
                                    if (t.getCause().getClass() == TimeoutException.class) {
                                        timeoutExceptionReached = true;
                                    }
                                    handleBatchException(numLogMessages, isDraining, t);
                                } else {
                                    timerTask.cancel(true);
                                    for(KafkaWritingTaskFuture f : committableBuckets.values()) {
                                        recordMetadataList.addAll(f.getRecordMetadataList());
                                    }
                                    onBatchComplete(numLogMessages, bucketFutures, isDraining);
                                }
                            }
                    );
            try {
                writerFuture.get();
            } catch (CompletionException | InterruptedException | ExecutionException e) {
                throw new LogStreamWriterException("Failed to write messages to topic " + topic, e);
            }
        }
    }

    @Test
    public void testWriteTimeoutAndRecovery() throws Exception {
        boolean isKeyed = false;
        String tempPath = getTempPath();
        String logStreamHeadFileName = "thrift.log";
        String path = FilenameUtils.concat(tempPath, logStreamHeadFileName);

        int oldestThriftLogIndex = 0;
        int readerBufferSize = 16000;
        int maxMessageSize = 16000;
        int processorBatchSize = 50;

        long processingIntervalInMillisMin = 1;
        long processingIntervalInMillisMax = 1;
        long processingTimeSliceInMilliseconds = 3600;
        int logRetentionInSecs = 15;

        // initialize a singer log config
        SingerLogConfig logConfig = new SingerLogConfig("test", tempPath, logStreamHeadFileName, null,
                                                        null, null);
        SingerLog singerLog = new SingerLog(logConfig);
        singerLog.getSingerLogConfig().setFilenameMatchMode(FileNameMatchMode.PREFIX);

        // initialize global variables in SingerSettings
        try {
            SingerConfig singerConfig = initializeSingerConfig(1, 1, Arrays.asList(logConfig));
            singerConfig.setSingerRestartConfig(new SingerRestartConfig().setRestartOnFailures(true));
            SingerSettings.initialize(singerConfig);
        } catch (Exception e) {
            e.printStackTrace();
            fail("got exception in test: " + e);
        }

        // initialize log stream
        LogStream logStream = new LogStream(singerLog, logStreamHeadFileName);
        LogStreamManager.addLogStream(logStream);
        SimpleThriftLogger<LogMessage> logger = new SimpleThriftLogger<>(path);
        KafkaProducerConfig producerConfig = new KafkaProducerConfig();
        producerConfig.setBrokerLists(Collections.singletonList(brokerEndpoint));
        KafkaWriter writer = new TestCommittableKafkaWriter(logStream,
                                                            producerConfig,
                                                            "topicx", false, false,
                                                            null, new Crc32ByteArrayPartitioner().getClass().getName(),
                                                            1, false);

        // initialize a log stream reader with 16K as readerBufferSize and
        // maxMessageSize
        LogStreamReader logStreamReader = new DefaultLogStreamReader(logStream,
                                                                     new ThriftLogFileReaderFactory(new ThriftReaderConfig(readerBufferSize, maxMessageSize)));
        // initialize a log stream processor that
        MemoryEfficientLogStreamProcessor processor = new MemoryEfficientLogStreamProcessor(logStream,
                                                                                            null, logStreamReader, writer, processorBatchSize, processingIntervalInMillisMin,
                                                                                            processingIntervalInMillisMax, processingTimeSliceInMilliseconds, logRetentionInSecs, false);

        try {
            // Save start position to watermark file.
            LogPosition startPosition = new LogPosition(logger.getLogFile(), logger.getByteOffset());
            WatermarkUtils.saveCommittedPositionToWatermark(
                    MemoryEfficientLogStreamProcessor.getWatermarkFilename(logStream), startPosition);

            List<LogMessage> messagesWritten = Lists.newArrayList();

            // Rotate log file while writing messages.
            for (int i = 0; i < 3; ++i) {
                rotateWithDelay(logger, 1000);
                List<LogMessageAndPosition> logMessageAndPositions = isKeyed
                        ? writeThriftLogMessages(logger, processorBatchSize + 20, 500, 50)
                        : writeThriftLogMessages(logger, processorBatchSize + 20, 500, 50);
                List<LogMessage> logMessages = getMessages(logMessageAndPositions);
                messagesWritten.addAll(logMessages);
            }

            // added to enable running this test on OS X
            while (logStream.isEmpty()) {
                Thread.sleep(1000);
                System.out.print(".");
            }

            // Process all message written so far.
            long numOfMessageProcessed = processor.processLogStream();

            // Write and process a single LogMessages.
            messagesWritten.addAll(getMessages(isKeyed ? writeThriftLogMessages(logger, 1, 500, 50)
                                                       : writeThriftLogMessages(logger, 1, 50)));

            // stop broker
            sharedKafkaTestResource.getKafkaBrokers().iterator().next().stop();

            // schedule start broker
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
            executor.schedule(new Runnable() {

                @Override
                public void run() {
                    try {
                        sharedKafkaTestResource.getKafkaBrokers().asList().get(0).start();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, 5, TimeUnit.SECONDS);

            // attempt to process log stream (should time out)
            long startTime = System.currentTimeMillis();
            long timeoutMs = 15000;
            boolean exceptionCaught = false;
            while (System.currentTimeMillis() - startTime < timeoutMs) {
                try {
                    numOfMessageProcessed = processor.processLogStream();
                    if (numOfMessageProcessed != 0) {
                        break;
                    }
                } catch (LogStreamProcessorException e) {
                    exceptionCaught = true;
                }
            }

            // batch timeout exception should be reached
            assertTrue(((TestCommittableKafkaWriter) writer).timeoutExceptionReached);

            // messages should have been processed when broker started up again
            assertTrue(numOfMessageProcessed > 0);

            // batch timeout should've been reached
            assertTrue(exceptionCaught);

            // should have no more processed
            numOfMessageProcessed = processor.processLogStream();
            assertEquals(0, numOfMessageProcessed);

            Properties consumerProperties = new Properties();
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroupId");
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerEndpoint);
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties);
            consumer.subscribe(Collections.singletonList("topicx"));

            ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofSeconds(5));

            List<byte[]> consumerRecordKeys = new ArrayList<>();

            consumerRecords.forEach(r -> consumerRecordKeys.add(r.key()));

            // every produced record's key should be in the consumed records list
            ((TestCommittableKafkaWriter) writer).producerRecordsList.forEach(pr -> {
                boolean found = false;
                for (byte[] consumerRecordKey: consumerRecordKeys) {
                    if (Arrays.equals(consumerRecordKey, pr.key())) {
                        found = true;
                        break;
                    }
                }
                assertTrue(found);
            });

            // check that all record metadata futures processed by writer is done non-exceptionally, meaning that it was acked by the server
            for (CompletableFuture<RecordMetadata> recordMetadataFuture: ((TestCommittableKafkaWriter) writer).recordMetadataList) {
                assertTrue(recordMetadataFuture.isDone());
                assertFalse(recordMetadataFuture.isCompletedExceptionally());
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail("Got exception in test");
        } finally {
            logger.close();
            processor.close();
        }
    }

    @Test
    public void testFlushDoesTimeout() throws Exception {
        boolean isKeyed = false;
        String tempPath = getTempPath();
        String logStreamHeadFileName = "thrift.log";
        String path = FilenameUtils.concat(tempPath, logStreamHeadFileName);

        int oldestThriftLogIndex = 0;
        int readerBufferSize = 16000;
        int maxMessageSize = 16000;
        int processorBatchSize = 50;

        long processingIntervalInMillisMin = 1;
        long processingIntervalInMillisMax = 1;
        long processingTimeSliceInMilliseconds = 3600;
        int logRetentionInSecs = 15;

        // initialize a singer log config
        SingerLogConfig logConfig = new SingerLogConfig("test", tempPath, logStreamHeadFileName, null,
                                                        null, null);
        SingerLog singerLog = new SingerLog(logConfig);
        singerLog.getSingerLogConfig().setFilenameMatchMode(FileNameMatchMode.PREFIX);

        // initialize global variables in SingerSettings
        try {
            SingerConfig singerConfig = initializeSingerConfig(1, 1, Arrays.asList(logConfig));
            singerConfig.setSingerRestartConfig(new SingerRestartConfig().setRestartOnFailures(true));
            SingerSettings.initialize(singerConfig);
        } catch (Exception e) {
            e.printStackTrace();
            fail("got exception in test: " + e);
        }

        // initialize log stream
        LogStream logStream = new LogStream(singerLog, logStreamHeadFileName);
        LogStreamManager.addLogStream(logStream);
        SimpleThriftLogger<LogMessage> logger = new SimpleThriftLogger<>(path);
        KafkaProducerConfig producerConfig = new KafkaProducerConfig();
        producerConfig.setBrokerLists(Collections.singletonList(brokerEndpoint));
        KafkaWriter writer = new TestCommittableKafkaWriter(logStream,
                                                            producerConfig,
                                                            "topicx", false, false,
                                                            null, new Crc32ByteArrayPartitioner().getClass().getName(),
                                                            1, false);

        // initialize a log stream reader with 16K as readerBufferSize and
        // maxMessageSize
        LogStreamReader logStreamReader = new DefaultLogStreamReader(logStream,
                                                                     new ThriftLogFileReaderFactory(
                                                                             new ThriftReaderConfig(readerBufferSize,
                                                                                                    maxMessageSize)));
        // initialize a log stream processor that
        MemoryEfficientLogStreamProcessor processor = new MemoryEfficientLogStreamProcessor(logStream,
                                                                                            null, logStreamReader,
                                                                                            writer, processorBatchSize,
                                                                                            processingIntervalInMillisMin,
                                                                                            processingIntervalInMillisMax,
                                                                                            processingTimeSliceInMilliseconds,
                                                                                            logRetentionInSecs, false);

        try {
            // Save start position to watermark file.
            LogPosition startPosition = new LogPosition(logger.getLogFile(), logger.getByteOffset());
            WatermarkUtils.saveCommittedPositionToWatermark(
                    MemoryEfficientLogStreamProcessor.getWatermarkFilename(logStream), startPosition);

            List<LogMessage> messagesWritten = Lists.newArrayList();

            // Rotate log file while writing messages.
            for (int i = 0; i < 3; ++i) {
                rotateWithDelay(logger, 1000);
                List<LogMessageAndPosition> logMessageAndPositions = isKeyed
                        ? writeThriftLogMessages(logger, processorBatchSize + 20, 500, 50)
                        : writeThriftLogMessages(logger, processorBatchSize + 20, 500, 50);
                List<LogMessage> logMessages = getMessages(logMessageAndPositions);
                messagesWritten.addAll(logMessages);
            }

            // added to enable running this test on OS X
            while (logStream.isEmpty()) {
                Thread.sleep(1000);
                System.out.print(".");
            }

            // Process all message written so far.
            long numOfMessageProcessed = processor.processLogStream();

            // Write and process a single LogMessages.
            messagesWritten.addAll(getMessages(isKeyed ? writeThriftLogMessages(logger, 1, 500, 50)
                                                       : writeThriftLogMessages(logger, 1, 50)));

            // stop broker
            sharedKafkaTestResource.getKafkaBrokers().iterator().next().stop();

            // flush should make this call stuck and timeout
            CompletableFuture<Void> timerFuture = new CompletableFuture<>();
            ScheduledExecutorService executionTimer = new ScheduledThreadPoolExecutor(1);
            Future<?> timerTask = executionTimer.schedule(() -> {
                timerFuture.completeExceptionally(new TimeoutException());
            }, 5, TimeUnit.SECONDS);
            CompletableFuture<Void> processorFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    processor.processLogStream();
                    return Void.TYPE.newInstance();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            CompletableFuture<Void> future = timerFuture.applyToEitherAsync(processorFuture, Function.identity())
                       .whenComplete(
                               (v, t) -> {
                                   assertNotNull(t);
                                   assertEquals(t.getCause().getClass(), TimeoutException.class);
                               }
            );

            try {
                future.get();
            } catch (Exception e) {
                assertEquals(e.getCause().getClass(), TimeoutException.class);
            }


        } catch (Exception e) {
            e.printStackTrace();
            fail("Got exception in test");
        } finally {
            logger.close();
            processor.close();
        }
    }

    private static List<LogMessage> getMessages(List<LogMessageAndPosition> messageAndPositions) {
        List<LogMessage> messages = Lists.newArrayListWithExpectedSize(messageAndPositions.size());
        for (LogMessageAndPosition messageAndPosition : messageAndPositions) {
            messages.add(messageAndPosition.getLogMessage());
        }
        return messages;
    }

    private SingerConfig initializeSingerConfig(int processorThreadPoolSize,
                                                int writerThreadPoolSize,
                                                List<SingerLogConfig> singerLogConfigs) {
        SingerConfig singerConfig = new SingerConfig();
        singerConfig.setThreadPoolSize(1);
        singerConfig.setWriterThreadPoolSize(1);
        singerConfig.setLogConfigs(singerLogConfigs);
        return singerConfig;
    }

    protected String getTempPath() {
        return tempDir.getRoot().getPath();
    }

    protected void rotateWithDelay(SimpleThriftLogger<LogMessage> logger, long delayInMillis)
            throws InterruptedException, IOException {
        // Sleep for a given period of time as file's lastModified timestamp is up to seconds on some platforms.
        Thread.sleep(delayInMillis);
        logger.rotate();
    }

    private byte[] getRandomByteArray(int size) {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    protected LogMessageAndPosition writeThriftLogMessage(
            SimpleThriftLogger<LogMessage> logger, int keySize, int messageSize) throws Exception {
        LogMessage message = new LogMessage(ByteBuffer.wrap(getRandomByteArray(messageSize)));
        message.setKey(getRandomByteArray(keySize));
        logger.logThrift(message);
        logger.flush();
        return new LogMessageAndPosition(
                message, new LogPosition(logger.getLogFile(), logger.getByteOffset()));
    }

    protected LogMessageAndPosition writeThriftLogMessage(
            SimpleThriftLogger<LogMessage> logger, int messageSize) throws Exception {
        LogMessage message = new LogMessage(ByteBuffer.wrap(getRandomByteArray(messageSize)));
        logger.logThrift(message);
        logger.flush();
        return new LogMessageAndPosition(
                message, new LogPosition(logger.getLogFile(), logger.getByteOffset()));
    }

    protected List<LogMessageAndPosition> writeThriftLogMessages(
            SimpleThriftLogger<LogMessage> logger, int n, int keySize, int messageSize) throws Exception {
        List<LogMessageAndPosition> logMessages = Lists.newArrayListWithExpectedSize(n);
        for (int j = 0; j < n; ++j) {
            logMessages.add(writeThriftLogMessage(logger, keySize, messageSize));
        }
        return logMessages;
    }

    protected List<LogMessageAndPosition> writeThriftLogMessages(
            SimpleThriftLogger<LogMessage> logger, int n, int messageSize) throws Exception {
        List<LogMessageAndPosition> logMessages = Lists.newArrayListWithExpectedSize(n);
        for (int j = 0; j < n; ++j) {
            logMessages.add(writeThriftLogMessage(logger, messageSize));
        }
        return logMessages;
    }
}
