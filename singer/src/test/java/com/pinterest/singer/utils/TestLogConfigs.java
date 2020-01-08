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

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import com.google.common.collect.ImmutableSet;

public class TestLogConfigs {

  @Rule
  public final ExpectedSystemExit exit = ExpectedSystemExit.none();

  @Test
  public void testBrokerChangeSystemExit() {
    ConcurrentMap<String, Set<String>> kafkaServerSets = new ConcurrentHashMap<>();
    kafkaServerSets.put("/xyz", new HashSet<>(Arrays.asList("one:9092", "two:9092", "three:9092")));
    BrokerSetChangeListener listener = new BrokerSetChangeListener("/xyz", kafkaServerSets, 100);
    listener.onChange(
        ImmutableSet.of(new InetSocketAddress("one", 9092),
            new InetSocketAddress("two", 9092),
            new InetSocketAddress("three", 9092)));
    assertTrue(true);
    listener.onChange(
        ImmutableSet.of(new InetSocketAddress("one", 9092),
            new InetSocketAddress("two", 9092)));
    assertTrue(true);
    exit.expectSystemExitWithStatus(0);
    listener.onChange(
        ImmutableSet.of(new InetSocketAddress("one", 9092),
            new InetSocketAddress("four", 9092)));
  }

}
