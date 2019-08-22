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
package com.pinterest.singer.writer.pulsar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.LogStreamWriter;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.common.errors.LogStreamWriterException;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.configuration.PulsarProducerConfig;
import com.pinterest.singer.thrift.configuration.PulsarWriterConfig;
import com.pinterest.singer.utils.SingerUtils;
import com.pinterest.singer.utils.StatsUtils;

/**
 * Pulsar Writer is capable of writing to a Pulsar cluster.1 Pulsar Writer
 * instance can write to only 1 topic. <br>
 * We use async writer to write the entire batch to Pulsar and then perform a
 * manual flush and wait for all {@link CompletableFuture} to be acknowledged
 * before returning. In case of failure the entire batch will be
 * reprocessed.<br>
 * <br>
 * 
 * Please note that the init method can throw an exception if we fail to
 * initialize either the Pulsar client or the Producer.<br>
 * <br>
 * 
 * E.g. configuration:<br>
 * writer.type=pulsar<br>
 * writer.pulsar.topic=persistent://tenant1/namespace1/b1<br>
 * writer.pulsar.producerConfig.compression.type=NONE<br>
 * writer.pulsar.producerConfig.serviceUrl=pulsar://pulsar01-broker-1:6650<br>
 */
public class PulsarWriter implements LogStreamWriter {

  private static final Logger LOG = LoggerFactory.getLogger(PulsarWriter.class);
  private static final String HOSTNAME = SingerUtils.getHostname();
  public static final String NUM_PULSAR_MESSAGES = SingerMetrics.SINGER_WRITER
      + "num_pulsar_messages_delivery_success";
  public static final String PULSAR_WRITE_FAILURE = SingerMetrics.SINGER_WRITER
      + "pulsar_write_failure";
  public static final String WRITER_BATCH_SIZE = SingerMetrics.SINGER_WRITER + "message_batch_size";
  public static final String PULSAR_THROUGHPUT = SingerMetrics.SINGER_WRITER
      + "topic_pulsar_throughput";
  public static final String PULSAR_LATENCY = SingerMetrics.SINGER_WRITER
      + "max_pulsar_batch_write_latency";
  private static Map<String, Producer<byte[]>> producerCache = new ConcurrentHashMap<>();
  private LogStream logStream;
  private Producer<byte[]> producer;
  private String topic;

  // pulsar topics have colons in the topic name, which is invalid in tsdb.
  // Use this string to publish pulsar related metrics.
  private String metricTag;
  private String logName;

  @SuppressWarnings("unchecked")
  public void validateConfig(PulsarProducerConfig producerConfig) throws ConfigurationException {
    try {
      CompressionType.valueOf(producerConfig.getCompressionType().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new ConfigurationException(
          "Invalid compression type:" + producerConfig.getCompressionType().toUpperCase(), e);
    }
    try {
      ((Class<PulsarMessagePartitioner>) Class.forName(producerConfig.getPartitionerClass()))
          .newInstance();
    } catch (Exception e) {
      throw new ConfigurationException(
          "Invalid partitioner class:" + producerConfig.getPartitionerClass());
    }

    if (producerConfig.getWriteTimeoutInSeconds() < 0) {
      throw new ConfigurationException("Write timeout can't be a negative number");
    }
  }

  public PulsarWriter init(LogStream logStream, PulsarWriterConfig writerConfig)
      throws ConfigurationException, LogStreamWriterException {
    PulsarProducerConfig producerConfig = writerConfig.getProducerConfig();
    validateConfig(producerConfig);
    this.logStream = logStream;
    this.logName = logStream.getLogStreamName();
    this.topic = writerConfig.getTopic();
    this.metricTag = StatsUtils.pulsarTopicToMetricTag(this.topic);

    if ((producer = producerCache.get(getProducerKey(producerConfig, topic))) == null) {
      synchronized (producerCache) {
        if ((producer = producerCache.get(getProducerKey(producerConfig, topic))) == null) {
          producer = createProducer(topic, logName, producerConfig);
          producerCache.put(getProducerKey(producerConfig, topic), producer);
          LOG.info("Created new Pulsar producer with pulsarServiceUrl:" + producerConfig.getServiceUrl()
              + " topic:" + topic + " logStream:" + logName);
        }
      }
    }
    return this;
  }

  @SuppressWarnings("unchecked")
  public static Producer<byte[]> createProducer(String topic, String logName, PulsarProducerConfig producerConfig) throws LogStreamWriterException {
    PulsarClient pulsarClient = null;
    try {
      pulsarClient = PulsarClient.builder().serviceUrl(producerConfig.getServiceUrl())
          .build();
    } catch (PulsarClientException e) {
      throw new LogStreamWriterException(
          "Failed to build Pulsar client with service URL:" + producerConfig.getServiceUrl(),
          e);
    }
    LOG.info("Created Pulsar client to connect to:" + producerConfig.getServiceUrl()
        + " topic:" + topic + " logStream:" + logName);
    try {
      Producer<byte[]> producer = pulsarClient.newProducer()
          .compressionType(
              CompressionType.valueOf(producerConfig.getCompressionType().toUpperCase()))
          .messageRoutingMode(MessageRoutingMode.CustomPartition)
          .messageRouter(new PulsarMessageRouter((Class<PulsarMessagePartitioner>) Class
              .forName(producerConfig.getPartitionerClass())))
          .sendTimeout(producerConfig.getWriteTimeoutInSeconds(), TimeUnit.SECONDS)
          .topic(topic).blockIfQueueFull(true)
          .batchingMaxMessages(producerConfig.getBatchingMaxMessages())
          .maxPendingMessages(producerConfig.getMaxPendingMessages())
          .batchingMaxPublishDelay(producerConfig.getBatchingMaxPublishDelayInMilli(),
              TimeUnit.MILLISECONDS)
          .enableBatching(true).maxPendingMessagesAcrossPartitions(
              producerConfig.getMaxPendingMessagesAcrossPartitions())
          .create();
      return producer;
    } catch (PulsarClientException | InstantiationException | IllegalAccessException
        | ClassNotFoundException e) {
      throw new LogStreamWriterException("Failed to initialize Pulsar writer", e);
    }
  }

  public static String getProducerKey(PulsarProducerConfig producerConfig, String topic) {
    return producerConfig.getServiceUrl() + "__" + topic;
  }

  @Override
  public void close() throws IOException {
    producer.flush();
  }

  @Override
  public LogStream getLogStream() {
    return logStream;
  }

  @Override
  public boolean isAuditingEnabled() {
    return false;
  }

  @Override
  public void writeLogMessages(List<LogMessage> messages) throws LogStreamWriterException {
    long bytesWritten = 0;
    List<CompletableFuture<MessageId>> messsageFutures = new ArrayList<>();
    long maxPulsarWriteLatency = System.currentTimeMillis();
    for (LogMessage m : messages) {
      TypedMessageBuilder<byte[]> message = producer.newMessage();
      if (m.isSetKey()) {
        message.keyBytes(m.getKey());
        bytesWritten += m.getKey().length;
      }
      CompletableFuture<MessageId> sendAsync = message.value(m.getMessage()).sendAsync();
      messsageFutures.add(sendAsync);
      bytesWritten += m.getMessage().length;
    }
    try {
      producer.flush();
      for (CompletableFuture<MessageId> future : messsageFutures) {
        future.get();
      }
    } catch (PulsarClientException | InterruptedException | ExecutionException e) {
      OpenTsdbMetricConverter.incr(PULSAR_WRITE_FAILURE, messages.size(), "topic=" + metricTag,
          "host=" + HOSTNAME, "logname=" + logName);
      throw new LogStreamWriterException("Message delivery failed", e);
    }

    maxPulsarWriteLatency = System.currentTimeMillis() - maxPulsarWriteLatency;
    OpenTsdbMetricConverter.gauge(PULSAR_THROUGHPUT, bytesWritten, "topic=" + metricTag,
        "host=" + HOSTNAME, "logname=" + logName);
    OpenTsdbMetricConverter.gauge(PULSAR_LATENCY, maxPulsarWriteLatency, "topic=" + metricTag,
        "host=" + HOSTNAME, "logname=" + logName);
    OpenTsdbMetricConverter.incr(NUM_PULSAR_MESSAGES, messages.size(), "topic=" + metricTag,
        "host=" + HOSTNAME, "logname=" + logName);
    LOG.info("Completed batch writes to Pulsar topic:" + metricTag + " size:" + messages.size());

  }

  protected void setProducer(Producer<byte[]> producer) {
    this.producer = producer;
  }

}