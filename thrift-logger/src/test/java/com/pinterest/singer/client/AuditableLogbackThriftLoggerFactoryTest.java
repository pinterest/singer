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

import static org.junit.jupiter.api.Assertions.*;

import com.pinterest.singer.client.logback.AuditableLogbackThriftLoggerFactory;
import com.pinterest.singer.thrift.Event;
import com.pinterest.singer.thrift.ThriftMessage;

import org.junit.jupiter.api.Test;
import java.io.File;

public class AuditableLogbackThriftLoggerFactoryTest {

  @Test
  public void testAuditLogbackThriftFactoryInitialization() {
    //ThriftLoggerFactory.initializeAuditLogbackThriftLoggerFactory();
    ThriftLoggerFactory.initialize();
    assertTrue(ThriftLoggerFactory.getThriftLoggerFactoryInstance().getClass().isAssignableFrom
        (AuditableLogbackThriftLoggerFactory.class));
    ThriftLoggerFactory.getThriftLoggerFactoryInstance().shutdown();
  }

  @Test
  public void testFactoryCreateAuditLogbackThriftLogger() {
   // ThriftLoggerFactory.initializeAuditLogbackThriftLoggerFactory();
    ThriftLoggerFactory.initialize();
    ThriftLogger l1 = ThriftLoggerFactory.getLogger(newConfig("topic1", ThriftMessage.class));
    assertTrue(l1.getClass().isAssignableFrom(AuditableLogbackThriftLogger.class));
    ThriftLoggerFactory.getThriftLoggerFactoryInstance().shutdown();
  }

  @Test
  public void testFactoryRemembersThriftLoggers() {
    // Make sure that the factory creates a new logger
    // for each topic and reuses the same object on subsequent calls.
   // ThriftLoggerFactory.initializeAuditLogbackThriftLoggerFactory();
    ThriftLoggerFactory.initialize();
    ThriftLogger l1 = ThriftLoggerFactory.getLogger(newConfig("topic1", ThriftMessage.class));
    ThriftLogger l2 = ThriftLoggerFactory.getLogger(newConfig("topic1", ThriftMessage.class));
    ThriftLogger l3 = ThriftLoggerFactory.getLogger(newConfig("topic2", Event.class));
    assertSame(l1, l2);
    assertNotSame(l1, l3);
    ThriftLoggerFactory.getThriftLoggerFactoryInstance().shutdown();
  }

  @Test
  public void testSleepInSecBeforeCloseLoggers(){
    ThriftLoggerFactory.initialize();
    AuditableLogbackThriftLoggerFactory factory = (AuditableLogbackThriftLoggerFactory)
        ThriftLoggerFactory.getThriftLoggerFactoryInstance();
    assertEquals(-1, factory.getSleepInSecBeforeCloseLoggers());
    factory.setSleepInSecBeforeCloseLoggers(5);
    assertEquals(5, factory.getSleepInSecBeforeCloseLoggers());
    factory.shutdown();
  }

  public ThriftLoggerConfig newConfig(String topic, Class<?> thriftClazz ){
    int retentionSecs = 96 * 60 * 60;
    int thresholdBytes = 100 * 1024;
    String prefixPath = "/tmp/log";
    File baseDir = new File(prefixPath, topic);
    return new ThriftLoggerConfig(baseDir, topic, retentionSecs, thresholdBytes, thriftClazz, true);
  }

}
