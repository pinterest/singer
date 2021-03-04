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
import com.pinterest.singer.writer.KafkaMessagePartitioner;

import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * First byte as integer partitioner for byte array key.
 *
 * This partitioner will:
 * 1) return a random partition between 0 and numOfPartitions when the key is null.
 * 2) throw runtime exception when key is not byte array type
 * 3) read the first four bytes of the key as a byte array and compose it into an integer assuming big endian order and
 * return the modulo of numOfPartitions on the integer.
 */
public class FirstByteIntPartitioner implements KafkaMessagePartitioner {

    private static final Logger LOG = LoggerFactory.getLogger(
            FirstByteIntPartitioner.class);

    private final Random random;

    public FirstByteIntPartitioner() {
        random = new Random();
    }

    public int partition(Object object, List<PartitionInfo> partitions) {
        int numOfPartitions = partitions.size();
        Preconditions.checkArgument(numOfPartitions > 0);
        byte[] key = (byte[]) object;
        if (object == null) {
            return Math.abs(random.nextInt() % numOfPartitions);
        } else {
            ByteBuffer byteBuffer = ByteBuffer.wrap(key); // big-endian by default
            int intValue = byteBuffer.getInt(); // Read the first four bytes and compose it into an integer value
            LOG.debug("FirstByteIntPartitioner.partition producer intValue: {} on key: {}", intValue,
                    Arrays.toString(key));
            return intValue % numOfPartitions;
        }
    }
}
