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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.List;

import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partitions a string key using MD5 Hashing.
 * This partitioner is intended to have the same behavior as
 * com/pinterest/hadoop/manas/ManasDocumentKeyPartitioner.java in optimus.
 */
public class StringKeyMd5Partitioner implements KafkaMessagePartitioner {

  private static final Logger LOG = LoggerFactory.getLogger(StringKeyMd5Partitioner.class);
  private HashFunction hf = Hashing.md5();

  public int partition(Object object, List<PartitionInfo> partitions) {
    int numPartitions = partitions.size();
    Preconditions.checkArgument(numPartitions > 0);
    byte[] key = (byte[]) object;
    String strKey = new String((byte[]) key);
    int partitionNum = hf.newHasher(16).putString(strKey, Charsets.UTF_8).hash().asInt()
        % numPartitions;
    if (partitionNum < 0) {
      partitionNum += numPartitions;
    }
    LOG.debug(String.format("The partition number for key {} is {}", key, partitionNum));
    return partitionNum;
  }
}
