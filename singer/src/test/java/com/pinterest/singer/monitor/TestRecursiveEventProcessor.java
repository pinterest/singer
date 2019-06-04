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
package com.pinterest.singer.monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.Test;

import com.pinterest.singer.thrift.configuration.KubeConfig;
import com.pinterest.singer.thrift.configuration.SingerConfig;

public class TestRecursiveEventProcessor {

  private static SingerConfig singerConfig;

  @BeforeClass
  public static void beforeClass() {
    singerConfig = new SingerConfig();
    singerConfig.setKubernetesEnabled(true);
    KubeConfig kubeConfig = new KubeConfig();
    singerConfig.setKubeConfig(kubeConfig);
    kubeConfig.setPodLogDirectory(new File(".").getAbsolutePath() + "/target/pods");

    new File(kubeConfig.getPodLogDirectory()).mkdirs();
  }

  @Test
  public void testBlankInitialize() {
    LogStreamManager instance = LogStreamManager.getInstance();
    assertNotNull(instance);
    LogStreamManager.reset();
  }

  @Test
  public void testLogPathExtraction() {
    String path = RecursiveFSEventProcessor.extractRelativeLogPathFromAbsoluteEventPath(
        "/Users/test/kubernetes/target/logs/",
        new File("/Users/test/kubernetes/target/logs/b1a2a0c2-d3ab-11e7-a1db-0674200569b6/var/log").toPath(),
        "b1a2a0c2-d3ab-11e7-a1db-0674200569b6");
    assertEquals("/var/log", path);

    path = RecursiveFSEventProcessor.extractRelativeLogPathFromAbsoluteEventPath(
        "/Users/test/kubernetes/target/logs",
        new File("/Users/test/kubernetes/target/logs/b1a2a0c2-d3ab-11e7-a1db-0674200569b6/var/log").toPath(),
        "b1a2a0c2-d3ab-11e7-a1db-0674200569b6");
    assertEquals("/var/log", path);

    path = RecursiveFSEventProcessor.extractRelativeLogPathFromAbsoluteEventPath(
        "/Users/test/kubernetes/target//logs",
        new File("/Users/test/kubernetes/target/logs/b1a2a0c2-d3ab-11e7-a1db-0674200569b6/var/log").toPath(),
        "b1a2a0c2-d3ab-11e7-a1db-0674200569b6");
    assertEquals("/var/log", path);
  }

  @Test
  public void testExtractPodUidFromPath() {
    String uid = RecursiveFSEventProcessor.extractPodUidFromPath(new File(
        "/Users/test/git/singer/integration/kubernetes/target/logs/b1a2a0c2-d3ab-11e7-a1db-0674200569b6/var")
        .toPath(), "/Users/test/git/singer/integration/kubernetes/target/logs/");
    assertEquals("b1a2a0c2-d3ab-11e7-a1db-0674200569b6", uid);

    uid = RecursiveFSEventProcessor.extractPodUidFromPath(new File(
        "/Users/test/git/singer/integration/kubernetes/target/logs/b1a2a0c2-d3ab-11e7-a1db-0674200569b6/var")
        .toPath(), "/Users/test/git/singer/integration/kubernetes/target/logs");
    assertEquals("b1a2a0c2-d3ab-11e7-a1db-0674200569b6", uid);

    uid = RecursiveFSEventProcessor.extractPodUidFromPath(new File(
        "/Users/test/git/singer/integration/kubernetes/target/logs/b1a2a0c2-d3ab-11e7-a1db-0674200569b6/var/log")
        .toPath(), "/Users/test/git/singer/integration//kubernetes/target/logs");
    assertEquals("b1a2a0c2-d3ab-11e7-a1db-0674200569b6", uid);
  }

}
