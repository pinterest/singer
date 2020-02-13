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
package com.pinterest.singer.writer;

import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.utils.KafkaUtils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * KafkaProducerManager is a singleton that has a producer config -> kafka
 * producer mapping to maximize producer reuse.
 */
public class KafkaProducerManager {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerManager.class);
  private static KafkaProducerManager instance;
  private ConcurrentHashMap<KafkaProducerConfig, KafkaProducer<byte[], byte[]>> producers;

  protected KafkaProducerManager() {
    producers = new ConcurrentHashMap<>();
  }

  public static KafkaProducerManager getInstance() {
    if (instance == null) {
      synchronized (KafkaProducerManager.class) {
        if (instance == null) {
          instance = new KafkaProducerManager();
        }
      }
    }
    return instance;
  }

  public static KafkaProducer<byte[], byte[]> getProducer(KafkaProducerConfig config) {
    return KafkaProducerManager.getInstance().getProducerInternal(config);
  }

  public static boolean resetProducer(KafkaProducerConfig config) {
    return KafkaProducerManager.getInstance().resetProducerInternal(config);
  }

  public static void shutdown() {
    KafkaProducerManager.getInstance().shutdownInternal();
  }

  /**
   *  ngapi/ngapp often have >20 log streams that write to the same topic.
   *  Because of this, this method can be called concurrently when Singer starts.
   *  The following is the logic to avoid producer leakage:
   *    1. look up the concurrent map, if there is a not-null value for the key, return the value
   *    2. create a kafka @producer,
   *    3. call ConcurrentMap.putIfAbsent to put (config, producer) to the map
   *    4. if the return result of ConcurrentMap.putIfAbsent is not equal to @producer,
   *       close @producer to avoid tcp connection leakage
   */
  private KafkaProducer<byte[], byte[]> getProducerInternal(KafkaProducerConfig config) {
    KafkaProducer<byte[], byte[]> producer;
    KafkaProducer<byte[], byte[]> result;

    if (!producers.containsKey(config)) {
      producer = KafkaUtils.createKafkaProducer(config);
      result = producers.putIfAbsent(config, producer);
      if (result != null && result != producer) {
        producer.close();
      }
      // log metrics for no.of kafka producers currently in the cache
      OpenTsdbMetricConverter.addMetric(SingerMetrics.NUM_KAFKA_PRODUCERS, producers.size());
    }
    result = producers.get(config);
    return result;
  }
  
  
  public static void injectTestProducer(KafkaProducerConfig config, KafkaProducer<byte[], byte[]> producer) {
    KafkaProducerManager.getInstance().producers.put(config, producer);
  }

  /**
   * Reset the kafka producer
   * @param config  the kafka producer config
   * @return true if the reset operation succeed, otherwise return false.
   */
  private boolean resetProducerInternal(KafkaProducerConfig config) {
    boolean retval = false;
    KafkaProducer<byte[], byte[]> oldProducer = producers.get(config);
    if (oldProducer != null) {
      oldProducer.close();
      KafkaProducer<byte[], byte[]> newProducer = KafkaUtils.createKafkaProducer(config);
      retval = producers.replace(config, oldProducer, newProducer);
      if (!retval) {
        newProducer.close();
      }
      // log metrics for no.of kafka producers currently in the cache
      OpenTsdbMetricConverter.addMetric(SingerMetrics.NUM_KAFKA_PRODUCERS, producers.size());
    }
    return retval;
  }

  private void shutdownInternal() {
    for (KafkaProducer<byte[], byte[]> producer : producers.values()) {
      try {
        producer.close();
      } catch (Exception e) {
        LOG.error("Shutdown failure : ", e);
      }
    }
  }
  
  public Map<KafkaProducerConfig, KafkaProducer<byte[], byte[]>> getProducers() {
    return producers;
  }
}
