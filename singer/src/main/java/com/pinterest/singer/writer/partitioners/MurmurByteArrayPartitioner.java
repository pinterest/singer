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
package com.pinterest.singer.writer.partitioners;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.pinterest.singer.writer.KafkaMessagePartitioner;

public class MurmurByteArrayPartitioner implements KafkaMessagePartitioner {

  private static Logger LOG = LoggerFactory.getLogger(MurmurByteArrayPartitioner.class);

  private final HashFunction hashFunction;

  public MurmurByteArrayPartitioner() {
    hashFunction = Hashing.murmur3_32();
  }

  public int partition(Object object, List<PartitionInfo> partitions) {
    int numOfPartitions = partitions.size();
    if (object != null) {
      byte[] key = (byte[]) object;
      int hash = Math.abs(Math.abs(hashFunction.hashBytes(key).asInt()) % numOfPartitions);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Murmur partitioner produces hash: {} on key: {}", hash, Arrays.toString(key));
      }
      return hash;
    } else {
      // pick a random partition
      return ThreadLocalRandom.current().nextInt(numOfPartitions); 
    }
  }

}