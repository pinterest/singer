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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerLog;
import com.pinterest.singer.monitor.LogStreamManager;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;

import java.util.List;

public class TestSingerUtils {

  @Test
  public void testGetHostNameBasedOnConfig() {
    LogStream logStream = new LogStream(new SingerLog(new SingerLogConfig(), "pod-11"), "test");
    SingerConfig config = new SingerConfig();
    config.setKubernetesEnabled(true);
    // case 1 kubernetes enabled; pod id specified
    String hostNameBasedOnConfig = SingerUtils.getHostNameBasedOnConfig(logStream, config);
    assertEquals("pod-11", hostNameBasedOnConfig);
    
    // case 2 kubernetes enabled; pod id null
    logStream = new LogStream(new SingerLog(new SingerLogConfig(), null), "test");
    hostNameBasedOnConfig = SingerUtils.getHostNameBasedOnConfig(logStream, config);
    assertEquals(SingerUtils.getHostname(), hostNameBasedOnConfig);
    
    // case 3 kubernetes enabled; pod id NON_KUBERNETES_POD_ID
    logStream = new LogStream(new SingerLog(new SingerLogConfig(), LogStreamManager.NON_KUBERNETES_POD_ID), "test");
    hostNameBasedOnConfig = SingerUtils.getHostNameBasedOnConfig(logStream, config);
    assertEquals(SingerUtils.getHostname(), hostNameBasedOnConfig);
    
    // case 4 kubernetes disabled
    config.setKubernetesEnabled(false);
    logStream = new LogStream(new SingerLog(new SingerLogConfig(), "pod-11"), "test");
    hostNameBasedOnConfig = SingerUtils.getHostNameBasedOnConfig(logStream, config);
    assertEquals(SingerUtils.getHostname(), hostNameBasedOnConfig);
  }

  @Test
  public void testGetHostnamePrefixes() {
    // Check simple dashes only
    String regex = "-";
    SingerUtils.setHostname("localhost-prod-cluster-19970722", regex);
    String [] prefixes = {"localhost", "localhost-prod", "localhost-prod-cluster", "localhost-prod-cluster-19970722"};
    List<String> finalPrefixes = SingerUtils.getHostnamePrefixes(regex);
    assertTrue(finalPrefixes.equals(java.util.Arrays.asList(prefixes)));
    
    // Check dots and dashes
    regex = "[.-]";
    SingerUtils.setHostname("localhost-prod.cluster-19970722", regex);
    prefixes = new String[]{"localhost", "localhost-prod", "localhost-prod-cluster", "localhost-prod-cluster-19970722"};
    finalPrefixes = SingerUtils.getHostnamePrefixes(regex);
    assertTrue(finalPrefixes.equals(java.util.Arrays.asList(prefixes)));

    // Check regex is empty
    regex = "";
    SingerUtils.setHostname("localhost-dev.19970722", regex);
    prefixes = new String []{"localhost-dev.19970722"};
    finalPrefixes = SingerUtils.getHostnamePrefixes(regex);
    assertTrue(finalPrefixes.equals(java.util.Arrays.asList(prefixes)));

    // Check regex is null
    regex = null;
    SingerUtils.setHostname("localhost-dev.19970722", regex);
    prefixes = new String []{"localhost-dev.19970722"};
    finalPrefixes = SingerUtils.getHostnamePrefixes(regex);
    assertTrue(finalPrefixes.equals(java.util.Arrays.asList(prefixes)));

    // Check regex is not matched
    regex = "abc";
    SingerUtils.setHostname("localhost-dev.19970722", regex);
    prefixes = new String []{"localhost-dev.19970722"};
    finalPrefixes = SingerUtils.getHostnamePrefixes(regex);
    assertTrue(finalPrefixes.equals(java.util.Arrays.asList(prefixes)));

    // reset hostname
    SingerUtils.setHostname(SingerUtils.getHostname(), "-");
  }

}
