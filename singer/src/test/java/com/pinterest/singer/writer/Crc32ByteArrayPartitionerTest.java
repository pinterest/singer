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

import junit.framework.TestCase;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;

/**
 * Created by wangxd on 5/6/14.
 */
public class Crc32ByteArrayPartitionerTest extends TestCase {

  private final Crc32ByteArrayPartitioner partitioner = new Crc32ByteArrayPartitioner();

  @Test
  public void testCrc32ByteArrayPartitionerTestWithByteArray() {
    List<PartitionInfo> partitions = Arrays.asList(new PartitionInfo[60]);
    assertEquals(40, partitioner.partition(new byte[]{0, 80, 64, 36, 43, 101, 22, 13}, partitions));
  }

  @Test
  public void testCrc32ByteArrayPartitionerTestWithByteArrayWithIntMinHash() {
    List<PartitionInfo> partitions = Arrays.asList(new PartitionInfo[60]);
    // byte[]{0, 32, 69, -4, -59, 25, -32, 22} will produce a INT_MIN hash.
    assertEquals(8, partitioner.partition(new byte[]{0, 32, 69, -4, -59, 25, -32, 22}, partitions));
  }

  @Test
  public void testCrc32ByteArrayPartitionerTestWithWrongKeyType() {
    List<PartitionInfo> partitions = Arrays.asList(new PartitionInfo[60]);
    try {
      partitioner.partition(new Byte[]{1, 2}, partitions);
      fail("Should throw runtime exception on wrong key type");
    } catch (Throwable e) {
      assertTrue(e instanceof ClassCastException);
    }
  }
  
}
