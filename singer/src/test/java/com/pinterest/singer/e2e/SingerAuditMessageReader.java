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
package com.pinterest.singer.e2e;


import com.pinterest.singer.thrift.AuditMessage;
import com.pinterest.singer.utils.SingerTestHelper;
import com.pinterest.singer.utils.SingerUtils;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.thrift.TDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class SingerAuditMessageReader implements Runnable {

  private String auditTopic;
  private boolean stopped;
  private int auditCount;
  private long startTimestampInMillis;
  private Thread thread = null;

  public SingerAuditMessageReader(String topic, long startTimestampInMillis) {
    this.auditTopic = topic;
    this.stopped = true;
    this.auditCount = 0;
    this.startTimestampInMillis = startTimestampInMillis;
  }

  public void start() {
    thread = new Thread(this);
    thread.start();
  }

  public void stop() {
    stopped = true;
  }

  public void destroy() {
  }

  public int getAuditCount() {
    return auditCount;
  }

  public void run() {
    stopped = false;
    KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
    try {
      // read audit messages from kafka
      Properties properties = SingerTestHelper.createKafkaConsumerConfig();

      kafkaConsumer = new KafkaConsumer(properties);
      kafkaConsumer.subscribe(Arrays.asList(auditTopic));

      String hostName = SingerUtils.getHostname();
      System.out.println("Fetching auditing messages from " + hostName + " : ");
      TDeserializer deserializer = new TDeserializer();
      while (!stopped) {
        ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<byte[], byte[]> record : records) {
          byte[] bytes = record.value();
          AuditMessage auditMessage = new AuditMessage();
          deserializer.deserialize(auditMessage, bytes);
          if (auditMessage.hostname.equals(hostName) &&
              auditMessage.timestamp >= startTimestampInMillis) {
            this.auditCount += auditMessage.numMessages;
            System.out.println("num messages = " + auditCount + "  " + auditMessage);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (kafkaConsumer != null) {
        kafkaConsumer.close();
      }
    }
  }

  public static void main(String[] args) {
    SingerAuditMessageReader auditMessageReader =
        new SingerAuditMessageReader("singer_test_event_audit",
            System.currentTimeMillis() - 100000000L);
    auditMessageReader.run();
  }
}
