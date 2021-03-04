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

import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;

public class TestFirstByteIntPartitioner extends TestCase {

    private final FirstByteIntPartitioner partitioner = new FirstByteIntPartitioner();

    @Test
    public void testFirstByteIntPartitionerWithByteArray() {
        List<PartitionInfo> partitions = Arrays.asList(new PartitionInfo[30]);
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(15);
        assertEquals(15, partitioner.partition(byteBuffer.array(), partitions));
    }

    @Test
    public void testFirstByteIntPartitionerWithByteArrayModulo() {
        List<PartitionInfo> partitions = Arrays.asList(new PartitionInfo[30]);
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(79);
        assertEquals(19, partitioner.partition(byteBuffer.array(), partitions));
    }

    @Test
    public void testFirstByteIntPartitionerWithByteArrayReadFirstOnly() {
        List<PartitionInfo> partitions = Arrays.asList(new PartitionInfo[30]);
        ByteBuffer byteBuffer = ByteBuffer.allocate(12);
        byteBuffer.putInt(2).putInt(4).putInt(6);
        assertEquals(2, partitioner.partition(byteBuffer.array(), partitions));
    }

    @Test
    public void testFirstByteIntPartitionerWithWrongKeyType() {
        List<PartitionInfo> partitions = Arrays.asList(new PartitionInfo[60]);
        try {
            partitioner.partition(new Byte[]{1, 2}, partitions);
            fail("Should throw runtime exception on wrong key type");
        } catch (Throwable e) {
            assertTrue(e instanceof ClassCastException);
        }
    }

}
