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
package com.pinterest.singer.reader.mapped;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TByteBuffer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.BeforeClass;
import org.junit.Test;

import com.pinterest.singer.client.ThriftCodec;
import com.pinterest.singer.client.ThriftLogger;
import com.pinterest.singer.client.ThriftLoggerConfig;
import com.pinterest.singer.client.ThriftLoggerFactory;
import com.pinterest.singer.reader.ByteOffsetInputStream;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.utils.SingerUtils;

public class TestMappedTBinaryProtocol {

  private static final String BASE_DIR = "target/mappedreads";

  @BeforeClass
  public static void beforeClass() {
    File baseDir = new File(BASE_DIR);
    SingerUtils.deleteRecursively(baseDir);
  }

  @Test
  public void testSimpleReadWrite() throws TException {
    LogMessage msg = new LogMessage();
    String key = "hellow world";
    msg.setKey(key.getBytes());
    String value = "abcdef";
    msg.setMessage(value.getBytes());
    long checksum = 1233432L;
    msg.setChecksum(checksum);
    long nanoTime = System.nanoTime();
    msg.setTimestampInNanos(nanoTime);
    byte[] serialize = ThriftCodec.getInstance().serialize(msg);

    TProtocol pr = new TBinaryProtocol(new TByteBuffer(ByteBuffer.wrap(serialize)));
    LogMessage lm = new LogMessage();
    lm.read(pr);
    assertEquals(nanoTime, lm.getTimestampInNanos());
    assertEquals(key, new String(lm.getKey()));
    assertEquals(value, new String(lm.getMessage()));
    assertEquals(checksum, lm.getChecksum());

  }

  @Test
  public void testSimpleMappedReadWrite() throws Exception {
    String key = "hellow world";
    String value = "abcdefdfsfadsfrwqerfwerwe";
    int COUNT = 1000;
    for (int i = 0; i < 10; i++) {
      value += value;
    }
    long nanoTime = System.nanoTime();
    String dir = BASE_DIR;
    File baseDir = new File(dir);
    baseDir.mkdirs();
    String file = "simple";
    ThriftLoggerConfig thriftLoggerConfig = new ThriftLoggerConfig(baseDir, file, 1000,
        1024 * 1024 * 1024);
    ThriftLoggerFactory.initialize();
    ThriftLogger logger = ThriftLoggerFactory.getLogger(thriftLoggerConfig);
    for (int i = 0; i < COUNT; i++) {
      logger.append(key.getBytes(), value.getBytes(), nanoTime);
    }
    logger.close();

    File dataFile = new File(baseDir, file);
    long ts = System.nanoTime();
    MappedFileTBinaryProtocol pr1 = new MappedFileTBinaryProtocol(dataFile);
    for (int i = 0; i < COUNT; i++) {
      LogMessage lm = new LogMessage();
      lm.read(pr1);
      assertEquals(nanoTime, lm.getTimestampInNanos());
      assertEquals(key, new String(SingerUtils.readFromByteBuffer(lm.BufferForKey())));
      assertEquals(value, new String(SingerUtils.readFromByteBuffer(lm.BufferForMessage())));
    }
    ts = System.nanoTime() - ts;
    System.out.println("MappedThriftReader:" + ts / 1000 + "us");

    ts = System.nanoTime();
    TProtocol pr = new TBinaryProtocol(new TFramedTransport(
        new TIOStreamTransport(
            new ByteOffsetInputStream(new RandomAccessFile(dataFile, "r"), 1024 * 1024)),
        value.length() + 512));

    for (int i = 0; i < COUNT; i++) {
      LogMessage lm = new LogMessage();
      lm.read(pr);
      assertEquals(nanoTime, lm.getTimestampInNanos());
      assertEquals(key, new String(lm.getKey()));
      assertEquals(value, new String(lm.getMessage()));
    }
    ts = System.nanoTime() - ts;
    System.out.println("ThriftReader:" + ts / 1000 + "us");

  }

  @Test
  public void testMappedContinuousReads() throws Exception {
    String key = "hellow world";
    String value = "abcdefdfsfadsfrwqerfwerwe";
    int COUNT = 1000;
    for (int i = 0; i < 10; i++) {
      value += value;
    }
    long nanoTime = System.nanoTime();
    String dir = BASE_DIR;
    File baseDir = new File(dir);
    baseDir.mkdirs();
    String file = "continuous";
    ThriftLoggerConfig thriftLoggerConfig = new ThriftLoggerConfig(baseDir, file, 1000,
        1024 * 1024 * 500);
    ThriftLoggerFactory.initialize();
    ThriftLogger logger = ThriftLoggerFactory.getLogger(thriftLoggerConfig);
    for (int i = 0; i < COUNT; i++) {
      logger.append(key.getBytes(), value.getBytes(), nanoTime);
    }
    logger.close();

    File dataFile = new File(baseDir, file);
    TProtocol pr = new TBinaryProtocol(new TFramedTransport(
        new TIOStreamTransport(
            new ByteOffsetInputStream(new RandomAccessFile(dataFile, "r"), 1024 * 1024)),
        value.length() + 512));

    for (int i = 0; i < COUNT; i++) {
      LogMessage lm = new LogMessage();
      lm.read(pr);
      assertEquals(nanoTime, lm.getTimestampInNanos());
      assertEquals(key, new String(lm.getKey()));
      assertEquals(value, new String(lm.getMessage()));
    }

    TProtocol pr1 = new MappedFileTBinaryProtocol(dataFile);
    for (int i = 0; i < COUNT; i++) {
      LogMessage lm = new LogMessage();
      lm.read(pr1);
      assertEquals(nanoTime, lm.getTimestampInNanos());
      assertEquals(key, new String(SingerUtils.readFromByteBuffer(lm.BufferForKey())));
      assertEquals(value, new String(SingerUtils.readFromByteBuffer(lm.BufferForMessage())));
    }
  }

  @Test
  public void testConcurrentReads() throws Exception {
    String key = "hellow world";
    String value = "abcdefdfsfadsfrwqerfwerwe";
    int COUNT = 1000;
    for (int i = 0; i < 10; i++) {
      value += value;
    }
    long nanoTime = System.nanoTime();
    String dir = BASE_DIR;
    File baseDir = new File(dir);
    baseDir.mkdirs();
    String file = "concurrent";

    File dataFile = new File(baseDir, file);

    ThriftLoggerConfig thriftLoggerConfig = new ThriftLoggerConfig(baseDir, file, 1000,
        1024 * 1024 * 500);
    ThriftLoggerFactory.initialize();
    ThriftLogger logger = ThriftLoggerFactory.getLogger(thriftLoggerConfig);
    for (int i = 0; i < COUNT; i++) {
      logger.append(key.getBytes(), value.getBytes(), nanoTime);
    }
    TProtocol pr1 = new MappedFileTBinaryProtocol(dataFile);
    for (int i = 0; i < COUNT; i++) {
      LogMessage lm = new LogMessage();
      lm.read(pr1);
      assertEquals(nanoTime, lm.getTimestampInNanos());
      assertEquals(key, new String(SingerUtils.readFromByteBuffer(lm.BufferForKey())));
      assertEquals(value, new String(SingerUtils.readFromByteBuffer(lm.BufferForMessage())));
    }
    logger = ThriftLoggerFactory.getLogger(thriftLoggerConfig);
    for (int i = 0; i < COUNT; i++) {
      logger.append(key.getBytes(), value.getBytes(), nanoTime);
    }
    logger.close();
    for (int i = 0; i < COUNT; i++) {
      LogMessage lm = new LogMessage();
      lm.read(pr1);
      assertEquals(nanoTime, lm.getTimestampInNanos());
      assertEquals(key, new String(SingerUtils.readFromByteBuffer(lm.BufferForKey())));
      assertEquals(value, new String(SingerUtils.readFromByteBuffer(lm.BufferForMessage())));
    }
  }

}