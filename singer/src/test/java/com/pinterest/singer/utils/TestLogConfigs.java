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
package com.pinterest.singer.utils;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import com.google.common.collect.ImmutableSet;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;
import com.twitter.thrift.Status;

public class TestLogConfigs {

  @Rule
  public final ExpectedSystemExit exit = ExpectedSystemExit.none();

  @Test
  public void testBrokerChangeSystemExit() {
    ConcurrentMap<String, Set<String>> kafkaServerSets = new ConcurrentHashMap<>();
    kafkaServerSets.put("/xyz", new HashSet<>(Arrays.asList("one:9092", "two:9092", "three:9092")));
    BrokerSetChangeListener listener = new BrokerSetChangeListener("/xyz", kafkaServerSets, 100);
    listener.onChange(
        ImmutableSet.of(new ServiceInstance(new Endpoint("one", 9092), null, Status.ALIVE),
            new ServiceInstance(new Endpoint("two", 9092), null, Status.ALIVE),
            new ServiceInstance(new Endpoint("three", 9092), null, Status.ALIVE)));
    assertTrue(true);
    listener.onChange(
        ImmutableSet.of(new ServiceInstance(new Endpoint("one", 9092), null, Status.ALIVE),
            new ServiceInstance(new Endpoint("two", 9092), null, Status.ALIVE)));
    assertTrue(true);
    exit.expectSystemExitWithStatus(0);
    listener.onChange(
        ImmutableSet.of(new ServiceInstance(new Endpoint("one", 9092), null, Status.ALIVE),
            new ServiceInstance(new Endpoint("four", 9092), null, Status.ALIVE)));
  }

}
