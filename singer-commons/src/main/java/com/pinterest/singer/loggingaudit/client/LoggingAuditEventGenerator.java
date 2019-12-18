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

package com.pinterest.singer.loggingaudit.client;

import com.pinterest.singer.loggingaudit.thrift.LoggingAuditEvent;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditHeaders;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditStage;
import com.pinterest.singer.loggingaudit.thrift.configuration.AuditConfig;

import java.util.concurrent.ConcurrentHashMap;

public class LoggingAuditEventGenerator {

  private final String host;
  private final LoggingAuditStage stage;
  private ConcurrentHashMap<String, AuditConfig> auditConfigs;

  public LoggingAuditEventGenerator(String host, LoggingAuditStage stage,
                                    ConcurrentHashMap<String, AuditConfig> auditConfigs) {
    this.host = host;
    this.stage = stage;
    this.auditConfigs = auditConfigs;
  }

  public LoggingAuditEvent generateAuditEvent(String loggingAuditName,
                                              LoggingAuditHeaders headers,
                                              boolean messageValid,
                                              long messageAcknowledgedTimestamp,
                                              String kafkaCluster,
                                              String topic) {
    return new LoggingAuditEvent().setHost(host).setStage(stage)
        .setStartAtCurrentStage(auditConfigs.get(loggingAuditName).isStartAtCurrentStage())
        .setStopAtCurrentStage(auditConfigs.get(loggingAuditName).isStopAtCurrentStage())
        .setLoggingAuditHeaders(headers)
        .setHeaderGeneratedTimestamp(headers.getTimestamp())
        .setMessageValid(messageValid)
        .setMessageAcknowledgedTimestamp(messageAcknowledgedTimestamp)
        .setKafkaCluster(kafkaCluster)
        .setTopic(topic)
        .setAuditEventGeneratedTimestamp(System.currentTimeMillis());
  }

}
