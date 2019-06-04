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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.common.PartitionInfo;

import com.google.common.base.Charsets;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@StringKeyMd5Partitioner}
 */
public class StringKeyMd5PartitionerTest {

  private StringKeyMd5Partitioner partitioner;

  @Before
  public void setup() {
    partitioner = new StringKeyMd5Partitioner();
  }

  @Test
  public void testPartitionWithStringKey() {
    List<PartitionInfo> partitions = Arrays.asList(new PartitionInfo[50]);
    String strKey = "399553798187727154";
    byte[] key = strKey.getBytes(Charsets.UTF_8);
    assertEquals(24, partitioner.partition(key, partitions));
  }

  @Test
  public void testPartitionWithWrongKeyType() {
    List<PartitionInfo> partitions = Arrays.asList(new PartitionInfo[50]);
    Integer intKey = new Integer(1);
    try {
      partitioner.partition(new Byte[]{intKey.byteValue()}, partitions);
      fail("Should throw runtime exception on wrong key type");
    } catch (Throwable e) {
      assertTrue(e instanceof ClassCastException);
    }
  }
}
