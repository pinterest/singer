/**
 * Copyright 2026 Pinterest, Inc.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Optional;

import org.junit.Test;

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerLog;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;

public class TestTopicTemplateResolver {

  private static final String POD_LOG_DIRECTORY = "/var/log/pods";
  private static final String POD_UID = "payments_api-7f8d5b7c6-mnopq_98765432-7654-4321";

  private LogStream buildPodStream(String podUid, String streamDir) {
    SingerLogConfig logConfig = new SingerLogConfig();
    logConfig.setName(podUid + "..container_logs");
    logConfig.setLogDir(streamDir);
    logConfig.setLogStreamRegex("0.log");
    SingerLog singerLog = new SingerLog(logConfig, podUid);
    singerLog.addMetadata("app", SingerUtils.getByteBuf("paymentservice"));
    singerLog.addMetadata("bad-value", SingerUtils.getByteBuf("has spaces!"));
    return new LogStream(singerLog, "0.log");
  }

  // ---- validateTemplate ----

  @Test
  public void testValidateAcceptsSupportedVariables() {
    TopicTemplateResolver.validateTemplate("logs_%{namespace}_%{container}");
    TopicTemplateResolver.validateTemplate("%{metadata:app}");
    TopicTemplateResolver.validateTemplate("pl.%{namespace}.%{metadata:singer-topic}");
  }

  @Test
  public void testValidateRejectsPodLevelIdentifiers() {
    for (String variable : new String[] { "podName", "podUid", "pod" }) {
      try {
        TopicTemplateResolver.validateTemplate("logs_%{" + variable + "}");
        fail("%{" + variable + "} should be rejected");
      } catch (IllegalArgumentException e) {
        assertTrue("error should explain cardinality concern for " + variable,
            e.getMessage().contains("cardinality"));
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateRejectsUnknownVariable() {
    TopicTemplateResolver.validateTemplate("logs_%{cluster}");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateRejectsMetadataWithoutKey() {
    TopicTemplateResolver.validateTemplate("logs_%{metadata}");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateRejectsArgumentOnNamespace() {
    TopicTemplateResolver.validateTemplate("logs_%{namespace:foo}");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateRejectsTemplateWithoutVariables() {
    TopicTemplateResolver.validateTemplate("static_topic");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateRejectsIllegalLiteralCharacters() {
    TopicTemplateResolver.validateTemplate("logs/%{namespace}");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateRejectsUnclosedVariable() {
    TopicTemplateResolver.validateTemplate("logs_%{namespace");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateRejectsEmptyTemplate() {
    TopicTemplateResolver.validateTemplate("");
  }

  // ---- resolve ----

  @Test
  public void testResolveNamespaceContainerAndMetadata() {
    LogStream stream = buildPodStream(POD_UID,
        POD_LOG_DIRECTORY + "/" + POD_UID + "/maincontainer");
    Optional<String> resolved = TopicTemplateResolver
        .resolve("logs_%{namespace}_%{container}", stream, POD_LOG_DIRECTORY);
    assertEquals("logs_payments_maincontainer", resolved.get());

    resolved = TopicTemplateResolver
        .resolve("pl.%{namespace}.%{metadata:app}", stream, POD_LOG_DIRECTORY);
    assertEquals("pl.payments.paymentservice", resolved.get());
  }

  @Test
  public void testResolveFailsForMissingMetadataKey() {
    LogStream stream = buildPodStream(POD_UID, POD_LOG_DIRECTORY + "/" + POD_UID + "/web");
    assertFalse(TopicTemplateResolver
        .resolve("logs_%{metadata:nonexistent}", stream, POD_LOG_DIRECTORY).isPresent());
  }

  @Test
  public void testResolveFailsForNonKubernetesStream() {
    SingerLogConfig logConfig = new SingerLogConfig();
    logConfig.setName("host_log");
    logConfig.setLogDir("/var/log/hostapp");
    // SingerLog without pod uid == host-level stream
    LogStream stream = new LogStream(new SingerLog(logConfig), "app.log");
    assertFalse(TopicTemplateResolver
        .resolve("logs_%{namespace}", stream, POD_LOG_DIRECTORY).isPresent());
  }

  @Test
  public void testResolveFailsForContainerAtPodRoot() {
    // stream directly at the pod root has no container path segment
    LogStream stream = buildPodStream(POD_UID, POD_LOG_DIRECTORY + "/" + POD_UID);
    assertFalse(TopicTemplateResolver
        .resolve("logs_%{container}", stream, POD_LOG_DIRECTORY).isPresent());
  }

  @Test
  public void testResolveFailsForMalformedPodDirectoryName() {
    // no namespace separator in the pod directory name
    LogStream stream = buildPodStream("nounderscore",
        POD_LOG_DIRECTORY + "/nounderscore/web");
    assertFalse(TopicTemplateResolver
        .resolve("logs_%{namespace}", stream, POD_LOG_DIRECTORY).isPresent());
  }

  @Test
  public void testResolveFailsForIllegalResolvedTopic() {
    LogStream stream = buildPodStream(POD_UID, POD_LOG_DIRECTORY + "/" + POD_UID + "/web");
    // metadata value contains characters that are illegal in a Kafka topic name
    assertFalse(TopicTemplateResolver
        .resolve("logs_%{metadata:bad-value}", stream, POD_LOG_DIRECTORY).isPresent());
  }

  @Test
  public void testContainerIsFirstSegmentBelowPodRoot() {
    // volume-mounted log layout: /var/log/pods/<pod>/var/log — "container" resolves to the
    // first path segment which is only meaningful with the kubelet stdout layout
    LogStream stream = buildPodStream(POD_UID, POD_LOG_DIRECTORY + "/" + POD_UID + "/var/log");
    assertEquals("logs_var",
        TopicTemplateResolver.resolve("logs_%{container}", stream, POD_LOG_DIRECTORY).get());
  }

  @Test
  public void testIsLegalTopicName() {
    assertTrue(TopicTemplateResolver.isLegalTopicName("logs_payments.web-1"));
    assertFalse(TopicTemplateResolver.isLegalTopicName(""));
    assertFalse(TopicTemplateResolver.isLegalTopicName("."));
    assertFalse(TopicTemplateResolver.isLegalTopicName(".."));
    assertFalse(TopicTemplateResolver.isLegalTopicName("has space"));
    StringBuilder tooLong = new StringBuilder();
    for (int i = 0; i < 250; i++) {
      tooLong.append('a');
    }
    assertFalse(TopicTemplateResolver.isLegalTopicName(tooLong.toString()));
  }
}
