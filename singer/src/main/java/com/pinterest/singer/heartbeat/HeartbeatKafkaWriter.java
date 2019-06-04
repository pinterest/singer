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
package com.pinterest.singer.heartbeat;

import com.pinterest.singer.common.HeartbeatWriter;
import com.pinterest.singer.common.SingerStatus;
import com.pinterest.singer.thrift.configuration.HeartbeatWriterConfig;
import com.pinterest.singer.utils.KafkaUtils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class HeartbeatKafkaWriter implements HeartbeatWriter {

  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatKafkaWriter.class);

  private final KafkaProducer kafkaProducer;
  private final String topic;

  public HeartbeatKafkaWriter(HeartbeatWriterConfig writerConfig) {
    kafkaProducer = KafkaUtils.createKafkaProducer(writerConfig.kafkaWriterConfig.producerConfig);
    this.topic = writerConfig.kafkaWriterConfig.getTopic();
  }

  @Override
  public void write(SingerStatus status) throws InterruptedException, ExecutionException {
    byte[] msg = status.toString().getBytes();
    ProducerRecord<byte[], byte[]> keyedMessage;
    keyedMessage = new ProducerRecord<>(topic, status.hostName.getBytes(), msg);
    kafkaProducer.send(keyedMessage).get();
  }

  @Override
  public void close() throws IOException {
    kafkaProducer.close();
  }
}
