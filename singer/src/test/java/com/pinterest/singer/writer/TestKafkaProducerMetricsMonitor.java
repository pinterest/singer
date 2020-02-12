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
package com.pinterest.singer.writer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Test;

import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.twitter.ostrich.stats.Stats;

public class TestKafkaProducerMetricsMonitor {

  @Test
  public void testSignatureTagExtractionTLS() {
    KafkaProducerConfig config = new KafkaProducerConfig("/var/serverset/discovery/kafka_tls/prod",
        Arrays.asList(), "-1");
    assertEquals("kafka_tls", KafkaProducerMetricsMonitor.convertSignatureToTag(config));
  }

  @Test
  public void testSignatureTagExtraction() {
    KafkaProducerConfig config = new KafkaProducerConfig("/var/serverset/discovery/kafka/prod",
        Arrays.asList(), "-1");
    assertEquals("kafka", KafkaProducerMetricsMonitor.convertSignatureToTag(config));
  }

  @Test
  public void testPublishMetrics() {
    KafkaProducerConfig config = new KafkaProducerConfig("/var/serverset/discovery.kafka.prod",
        Arrays.asList("localhost:9092"), "-1");
    KafkaProducerManager.getInstance().getProducers().clear();
    KafkaProducer<byte[], byte[]> producer = KafkaProducerManager.getProducer(config);
    KafkaProducerMetricsMonitor monitor = new KafkaProducerMetricsMonitor();
    monitor.publishKafkaProducerMetricsToOstrich();
    producer.close();
    for (String metricName : KafkaProducerMetricsMonitor.PRODUCER_METRICS_WHITELIST) {
      Object gauge = Stats.getGauge("kafkaproducer." + metricName + " cluster=kafka").get();
      assertNotNull(gauge);
    }
  }

}