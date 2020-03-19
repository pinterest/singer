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
package com.pinterest.singer.client;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import java.io.File;
import org.junit.jupiter.api.Test;

public class BaseThriftFactoryTest {

  private static class SampleFactory extends BaseThriftLoggerFactory {

    @Override
    protected ThriftLogger createLogger(String topic, int maxRetentionHours) {
      return new BaseThriftLogger() {
        @Override
        public void append(byte[] partitionKey, byte[] message, long timeNanos) {
        }

        @Override
        public void append(byte[] partitionKey, TBase thriftMessage, long timeNanos)
            throws TException {
        }

        @Override
        public void close() {
        }
      };
    }

    @Override
    protected ThriftLogger createLogger(ThriftLoggerConfig thriftLoggerConfig) {
      return new BaseThriftLogger() {
        @Override
        public void append(byte[] partitionKey, byte[] message, long timeNanos) {
        }

        @Override
        public void append(byte[] partitionKey, TBase thriftMessage, long timeNanos)
            throws TException {
        }

        @Override
        public void close() {
        }
      };
    }
  }

  @Test
  public void testFactoryRemembersThriftLoggers() {
    // Make sure that the factory creates a new logger
    // for each topic and reuses the same object on subsequent calls.
    SampleFactory factory = new SampleFactory();
    ThriftLogger l1 = factory.getLogger("foo1", 96);
    ThriftLogger l2 = factory.getLogger("foo2", 96);
    ThriftLogger l3 = factory.getLogger("foo3", 96);

    assertNotSame(l1, l2);
    assertNotSame(l1, l3);
    assertNotSame(l2, l3);

    assertSame(l1, factory.getLogger("foo1", 96));
    assertSame(l2, factory.getLogger("foo2", 96));
    assertSame(l3, factory.getLogger("foo3", 96));
  }

  @Test
  public void testFactoryRemembersThriftLoggers2() {
    // Make sure that the factory creates a new logger
    // for each topic and reuses the same object on subsequent calls.
    SampleFactory factory = new SampleFactory();
    ThriftLogger l1 = factory.getLogger(newConfig("foo1"));
    ThriftLogger l2 = factory.getLogger(newConfig("foo2"));
    ThriftLogger l3 = factory.getLogger(newConfig("foo3"));

    assertNotSame(l1, l2);
    assertNotSame(l1, l3);
    assertNotSame(l2, l3);

    assertSame(l1, factory.getLogger(newConfig("foo1")));
    assertSame(l2, factory.getLogger(newConfig("foo2")));
    assertSame(l3, factory.getLogger(newConfig("foo3")));
  }

  @Test
  public void testDifferentBaseDirsForSameTopic() {
    SampleFactory factory = new SampleFactory();
    int retentionSecs = 96 * 60 * 60;
    int thresholdBytes = 100 * 1024;
    String topic = "test";

    ThriftLogger logger1 = factory.getLogger(
        new ThriftLoggerConfig(new File("/tmp/test1"), topic, retentionSecs, thresholdBytes));
    ThriftLogger logger2 = factory.getLogger(
        new ThriftLoggerConfig(new File("/tmp/test2"), topic, retentionSecs, thresholdBytes));

    assertSame(logger1, logger2);
    factory.shutdown();
  }

  @Test
  public void testDifferentBaseDirsForDifferentTopics() {
    SampleFactory factory = new SampleFactory();
    int retentionSecs = 96 * 60 * 60;
    int thresholdBytes = 100 * 1024;
    String topic = "test";

    ThriftLogger logger1 = factory.getLogger(
        new ThriftLoggerConfig(new File("/tmp/test1"), topic + "1", retentionSecs, thresholdBytes));
    ThriftLogger logger2 = factory.getLogger(
        new ThriftLoggerConfig(new File("/tmp/test2"), topic + "2", retentionSecs, thresholdBytes));

    assertNotSame(logger1, logger2);
    factory.shutdown();
  }

  @Test
  public void testSleepInSecBeforeCloseLoggers(){
    SampleFactory factory = new SampleFactory();
    assertEquals(-1, factory.getSleepInSecBeforeCloseLoggers());
    factory.setSleepInSecBeforeCloseLoggers(5);
    assertEquals(5, factory.getSleepInSecBeforeCloseLoggers());
    factory.shutdown();
  }

  private ThriftLoggerConfig newConfig(String topic) {
    int retentionSecs = 96 * 60 * 60;
    int thresholdBytes = 100 * 1024;
    String f = "/tmp/test";
    return new ThriftLoggerConfig(new File(f), topic, retentionSecs, thresholdBytes);
  }
}
