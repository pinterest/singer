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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.utils.LogConfigUtils;

/**
 * Responsible for pulling metrics from {@link KafkaProducer} and copying them
 * to Ostrich so they can be accessed and forwarded. This helps provide
 * additional instrumentation on Singer and how it's performing.
 */
public class KafkaProducerMetricsMonitor implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerMetricsMonitor.class);
  public static final Set<String> PRODUCER_METRICS_WHITELIST = new HashSet<>(
      Arrays.asList("buffer-total-bytes", "buffer-available-bytes"));
  // sample every 60seconds
  private static final int SAMPLING_INTERVAL = 60_000;

  @Override
  public void run() {
    while (true) {
      try {
        publishKafkaProducerMetricsToOstrich();
      } catch (Exception e) {
        LOG.warn("Error publishing KafkaProducer metrics", e);
      }
      try {
        Thread.sleep(SAMPLING_INTERVAL);
      } catch (InterruptedException e) {
        LOG.warn("KafkaProducerMetricsMonitor thread interrupted, exiting");
        break;
      }
    }
  }

  @SuppressWarnings({ "deprecation" })
  protected void publishKafkaProducerMetricsToOstrich() {
    Map<KafkaProducerConfig, KafkaProducer<byte[], byte[]>> producers = KafkaProducerManager
        .getInstance().getProducers();
    for (Entry<KafkaProducerConfig, KafkaProducer<byte[], byte[]>> kafkaProducerEntry : producers
        .entrySet()) {
      KafkaProducerConfig key = kafkaProducerEntry.getKey();
      String signature = convertSignatureToTag(key);
      Map<MetricName, ? extends Metric> metrics = kafkaProducerEntry.getValue().metrics();
      for (Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
        if (PRODUCER_METRICS_WHITELIST.contains(entry.getKey().name())) {
          OpenTsdbMetricConverter.gauge("kafkaproducer." + entry.getKey().name(),
              entry.getValue().value(), "cluster=" + signature);
        }
      }
    }
  }

  public static String convertSignatureToTag(KafkaProducerConfig key) {
    return key.getKafkaClusterSignature()
        .replaceAll("(" + LogConfigUtils.DEFAULT_SERVERSET_DIR + "|discovery|/|prod|\\.)", "");
  }

}