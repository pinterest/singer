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

import java.lang.reflect.Field;
import java.util.Arrays;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Test;

import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.utils.KafkaUtils;

public class TestKafkaProducerManager {

  @Test
  public void testBufferMemoryAssignment() throws IllegalArgumentException, IllegalAccessException,
                                           NoSuchFieldException, SecurityException {
    Field totalMemory = KafkaProducer.class.getDeclaredField("totalMemorySize");
    KafkaProducer<byte[], byte[]> producer = null;
    long val = 0;
    KafkaProducerConfig config = null;

    // check default case
    config = new KafkaProducerConfig("/var/serverset/discovery.kafka.prod",
        Arrays.asList("localhost:9092"), "-1");

    KafkaProducerManager.getInstance().getProducers().clear();
    producer = KafkaProducerManager.getProducer(config);
    totalMemory.setAccessible(true);
    val = (long) totalMemory.get(producer);
    assertEquals(32 * 1024 * 1024, val);

    // check case with less than 1024 bytes memory
    KafkaProducerManager.getInstance().getProducers().clear();
    config = new KafkaProducerConfig("/var/serverset/discovery.kafka.prod",
        Arrays.asList("localhost:9092"), "-1");
    config.setBufferMemory(KafkaUtils.DEFAULT_PRODUCER_BUFFER_MEMORY - 1);
    producer = KafkaProducerManager.getProducer(config);
    val = (long) totalMemory.get(producer);
    assertEquals(32 * 1024 * 1024, val);

    // check case where override of 2048 is set
    KafkaProducerManager.getInstance().getProducers().clear();
    config = new KafkaProducerConfig("/var/serverset/discovery.kafka.prod",
        Arrays.asList("localhost:9092"), "-1");
    config.setBufferMemory(2048);
    producer = KafkaProducerManager.getProducer(config);
    val = (long) totalMemory.get(producer);
    assertEquals(2048, val);
  }

}
