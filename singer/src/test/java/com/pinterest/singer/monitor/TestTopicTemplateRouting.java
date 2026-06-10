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
package com.pinterest.singer.monitor;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerLog;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.thrift.configuration.KafkaProducerConfig;
import com.pinterest.singer.thrift.configuration.KafkaWriterConfig;
import com.pinterest.singer.thrift.configuration.KubeConfig;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.utils.SingerUtils;

/**
 * Tests the topic resolution precedence in DefaultLogMonitor:
 *  1. writer.kafka.topicTemplate when it fully resolves to a legal topic name
 *  2. writer.kafka.topic (with legacy \N capture-group expansion) otherwise
 */
public class TestTopicTemplateRouting {

  private static final String POD_LOG_DIRECTORY = "/var/log/pods";
  private static final String POD_UID = "payments_api-7f8d5b7c6-mnopq_98765432-7654-4321";

  @Before
  public void before() {
    SingerSettings.reset();
    SingerConfig singerConfig = new SingerConfig();
    singerConfig.setKubernetesEnabled(true);
    KubeConfig kubeConfig = new KubeConfig();
    kubeConfig.setPodLogDirectory(POD_LOG_DIRECTORY);
    singerConfig.setKubeConfig(kubeConfig);
    SingerSettings.setSingerConfig(singerConfig);
  }

  @After
  public void after() {
    SingerSettings.reset();
  }

  private LogStream buildContainerStream(String container) {
    SingerLogConfig logConfig = new SingerLogConfig();
    logConfig.setName(POD_UID + "..container_logs");
    logConfig.setLogDir(POD_LOG_DIRECTORY + "/" + POD_UID + "/" + container);
    logConfig.setLogStreamRegex("0\\.log");
    SingerLog singerLog = new SingerLog(logConfig, POD_UID);
    singerLog.addMetadata("app", SingerUtils.getByteBuf("paymentservice"));
    return new LogStream(singerLog, "0.log");
  }

  @Test
  public void testTemplateTakesPrecedenceWhenResolvable() throws Exception {
    LogStream stream = buildContainerStream("web");
    KafkaWriterConfig writerConfig =
        new KafkaWriterConfig("fallback_topic", new KafkaProducerConfig());
    writerConfig.setTopicTemplate("logs_%{namespace}_%{container}");
    assertEquals("logs_payments_web", DefaultLogMonitor.resolveTopic(stream,
        stream.getSingerLog().getSingerLogConfig(), writerConfig));
  }

  @Test
  public void testMetadataVariableResolution() throws Exception {
    LogStream stream = buildContainerStream("web");
    KafkaWriterConfig writerConfig =
        new KafkaWriterConfig("fallback_topic", new KafkaProducerConfig());
    writerConfig.setTopicTemplate("logs_%{metadata:app}");
    assertEquals("logs_paymentservice", DefaultLogMonitor.resolveTopic(stream,
        stream.getSingerLog().getSingerLogConfig(), writerConfig));
  }

  @Test
  public void testFallbackToStaticTopicWhenVariableUnresolvable() throws Exception {
    LogStream stream = buildContainerStream("web");
    KafkaWriterConfig writerConfig =
        new KafkaWriterConfig("fallback_topic", new KafkaProducerConfig());
    writerConfig.setTopicTemplate("logs_%{metadata:nonexistent}");
    assertEquals("fallback_topic", DefaultLogMonitor.resolveTopic(stream,
        stream.getSingerLog().getSingerLogConfig(), writerConfig));
  }

  @Test
  public void testFallbackForNonKubernetesStream() throws Exception {
    SingerLogConfig logConfig = new SingerLogConfig();
    logConfig.setName("host_log");
    logConfig.setLogDir("/var/log/hostapp");
    logConfig.setLogStreamRegex("app\\.log");
    LogStream stream = new LogStream(new SingerLog(logConfig), "app.log");
    KafkaWriterConfig writerConfig =
        new KafkaWriterConfig("fallback_topic", new KafkaProducerConfig());
    writerConfig.setTopicTemplate("logs_%{namespace}");
    assertEquals("fallback_topic",
        DefaultLogMonitor.resolveTopic(stream, logConfig, writerConfig));
  }

  @Test
  public void testLegacyCaptureGroupExpansionStillWorks() throws Exception {
    SingerLogConfig logConfig = new SingerLogConfig();
    logConfig.setName("host_log");
    logConfig.setLogDir("/var/log/hostapp");
    logConfig.setLogStreamRegex("app_(.*)\\.log");
    LogStream stream = new LogStream(new SingerLog(logConfig), "app_web.log");
    KafkaWriterConfig writerConfig =
        new KafkaWriterConfig("logs-\\1", new KafkaProducerConfig());
    assertEquals("logs-web",
        DefaultLogMonitor.resolveTopic(stream, logConfig, writerConfig));
  }
}
