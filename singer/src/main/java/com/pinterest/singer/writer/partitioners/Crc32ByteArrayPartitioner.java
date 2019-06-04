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

import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.pinterest.singer.writer.KafkaMessagePartitioner;

import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * CRC32 partitioner for byte array key.
 *
 * This partitioner will:
 * 1) return a random partition between 0 and numOfPartitions when the key is null.
 * 2) throw runtime exception when key is not byte array type
 * 3) use a CRC 32 hashing function on the byte array to get a hash code and return the modulo of
 * numOfPartitions on the hash code.
 */
public class Crc32ByteArrayPartitioner implements KafkaMessagePartitioner {

  private static final Logger LOG = LoggerFactory.getLogger(
      Crc32ByteArrayPartitioner.class);

  private final HashFunction hashFunction;
  private final Random random;

  public Crc32ByteArrayPartitioner() {
    hashFunction = Hashing.crc32();
    random = new Random();
  }

  public int partition(Object object, List<PartitionInfo> partitions) {
    int numOfPartitions = partitions.size();
    Preconditions.checkArgument(numOfPartitions > 0);
    byte[] key = (byte[]) object;
    if (object == null) {
      return Math.abs(random.nextInt() % numOfPartitions);
    } else {
      int hash = hashFunction.hashBytes(key).asInt();
      LOG.debug("Crc32ByteArrayPartitioner.partition producer hash: {} on key: {}", hash,
          Arrays.toString(key));
      // Apply Math.abs two times to make it compatible with partition id when hash is not INT_MIN
      // We need the outter Math.abs because Math.abs(INT_MIN) = INT_MIN.
      return Math.abs(Math.abs(hash) % numOfPartitions);
    }
  }
}
