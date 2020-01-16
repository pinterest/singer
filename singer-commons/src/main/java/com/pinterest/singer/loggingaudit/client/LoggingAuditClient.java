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

import com.pinterest.singer.loggingaudit.client.common.LoggingAuditClientMetrics;
import com.pinterest.singer.utils.CommonUtils;
import com.pinterest.singer.loggingaudit.client.utils.ConfigUtils;
import com.pinterest.singer.utils.KafkaUtils;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditEvent;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditHeaders;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditStage;
import com.pinterest.singer.loggingaudit.thrift.configuration.LoggingAuditClientConfig;
import com.pinterest.singer.loggingaudit.thrift.configuration.LoggingAuditEventSenderConfig;
import com.pinterest.singer.loggingaudit.thrift.configuration.AuditConfig;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 *  LoggingAuditClient is used by various systems to track topics being audited and send out audit
 *  events.  Systems importing the audit client library needs to pass the LoggingAuditClientConfig
 *  when creating LoggingAuditClient instance.
 */
public class LoggingAuditClient {

  private static Logger LOG = LoggerFactory.getLogger(LoggingAuditClient.class);

  /**
   *  logging audit stage. could be THRIFTLOGGER, SINGER, MERCED
   */
  private final LoggingAuditStage stage;

  /**
   *  host name
   */
  private final String host;

  private final String KAFKA_SENDER_NAME = "AuditEventKafkaSender";

  /**
   *  Sender dequeues audit events and send out to external data store (only support Kafka now).
   */
  private LoggingAuditEventSender sender;

  /**
   *  stores audit events.  Its methods are called by multiple threads.
   */
  private LinkedBlockingDeque<LoggingAuditEvent> queue;

  /**
   *  audit event generator
   */
  private LoggingAuditEventGenerator loggingAuditEventGenerator;

  /**
   *  config to initialize LoggingAuditClient
   */
  private LoggingAuditClientConfig config;

  /**
   *  time to wait when enqueue audit event. default is 0 which means, if queue is full, the event
   *  is dropped.
   */
  private int enqueueWaitInMilliseconds = 0;

  /**
   *  audit is not enabled by default. Only topics specified in auditConfigs are audited.
   */
  private boolean enableAuditForAllTopicsByDefault = false;

  /**
   *  track topics being audited
   */
  private ConcurrentHashMap<String, AuditConfig> auditConfigs = new ConcurrentHashMap<>();

  /**
   *  flag controls when enqueing audit events is enabled or not.
   */
  private AtomicBoolean enqueueEnabled = new AtomicBoolean(true);

  public LoggingAuditClient(LoggingAuditClientConfig config) {
    this(CommonUtils.getHostName(), config);
  }

  public LoggingAuditClient(String host, LoggingAuditClientConfig config) {
    this.host = host;
    this.config = config;
    this.queue = new LinkedBlockingDeque<>(config.getQueueSize());
    this.enqueueWaitInMilliseconds = config.enqueueWaitInMilliseconds;
    this.enableAuditForAllTopicsByDefault = config.isEnableAuditForAllTopicsByDefault();
    this.stage = config.getStage();
    this.auditConfigs.putAll(config.getAuditConfigs());
    this.loggingAuditEventGenerator =
        new LoggingAuditEventGenerator(this.host, this.stage, this.auditConfigs);
    initLoggingAuditEventSender(this.config.getSenderConfig(), this.queue);
  }


  public void initLoggingAuditEventSender(LoggingAuditEventSenderConfig config,
                                          LinkedBlockingDeque<LoggingAuditEvent> queue) {
    this.sender = new AuditEventKafkaSender(config.getKafkaSenderConfig(), queue, this.stage,
        this.host, KAFKA_SENDER_NAME);
    ((AuditEventKafkaSender) this.sender).setKafkaProducer(KafkaUtils.createKafkaProducer(
        config.getKafkaSenderConfig().getKafkaProducerConfig(), KAFKA_SENDER_NAME));
    this.sender.start();
  }


  public LoggingAuditClientConfig getConfig() {
    return config;
  }

  public ConcurrentHashMap<String, AuditConfig> getAuditConfigs() {
    return auditConfigs;
  }

  /**
   *  create and enqueue a LoggingAuditEvent if TopicAuditConfig exists for this topic/logName,
   *  enqueueEnabled is true and there is capacity available in the queue.
   * @param loggingAuditHeaders
   * @param messageValid
   * @param messageAcknowledgedTimestamp
   */
  public void audit(String loggingAuditName, LoggingAuditHeaders loggingAuditHeaders,
                    boolean messageValid, long messageAcknowledgedTimestamp) {
    audit(loggingAuditName, loggingAuditHeaders, messageValid, messageAcknowledgedTimestamp, "",
        "");
  }

  /**
   *  create and enqueue a LoggingAuditEvent if TopicAuditConfig exists for this topic/logName,
   *  enqueueEnabled is true and there is capacity available in the queue.
   * @param loggingAuditName
   * @param loggingAuditHeaders
   * @param messageValid
   * @param messageAcknowledgedTimestamp
   * @param kafkaCluster
   * @param topic
   */
  public void audit(String loggingAuditName, LoggingAuditHeaders loggingAuditHeaders,
                    boolean messageValid,
                    long messageAcknowledgedTimestamp, String kafkaCluster, String topic) {
    if (!this.auditConfigs.containsKey(loggingAuditName)) {
      OpenTsdbMetricConverter
          .incr(LoggingAuditClientMetrics.AUDIT_EVENT_WITHOUT_TOPIC_CONFIGURED_ERROR,
              "logName=" + loggingAuditHeaders.getLogName(), "host=" + host,
              "stage=" + stage.toString());
      return;
    }
    if (enqueueEnabled.get()) {
      LoggingAuditEvent loggingAuditEvent = loggingAuditEventGenerator.generateAuditEvent(
          loggingAuditName, loggingAuditHeaders, messageValid, messageAcknowledgedTimestamp,
          kafkaCluster, topic);
      boolean successful = false;
      try {
        // compared to put() which is blocking until available space in the queue, offer()
        // returns false for enqueuing object if there is still no space after waiting for the
        // specified the time period.
        successful =
            queue.offer(loggingAuditEvent, enqueueWaitInMilliseconds, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        OpenTsdbMetricConverter
            .incr(LoggingAuditClientMetrics.AUDIT_CLIENT_ENQUEUE_EXCEPTION, "host=" + host,
                "stage=" + stage.toString());
      }
      if (successful) {
        CommonUtils.reportQueueUsage(queue.size(), queue.remainingCapacity(), host, stage.toString());
        OpenTsdbMetricConverter.incr(LoggingAuditClientMetrics.AUDIT_CLIENT_ENQUEUE_EVENTS_ADDED,
            "logName=" + loggingAuditHeaders.getLogName(), "host=" + host,
            "stage=" + stage.toString());
      } else {
        OpenTsdbMetricConverter.incr(LoggingAuditClientMetrics.AUDIT_CLIENT_ENQUEUE_EVENTS_DROPPED,
            "logName=" + loggingAuditHeaders.getLogName(), "host=" + host,
            "stage=" + stage.toString());
      }
    } else {
      OpenTsdbMetricConverter
          .incr(LoggingAuditClientMetrics.AUDIT_CLIENT_ENQUEUE_DISABLED_EVENTS_DROPPED,
              "logName=" + loggingAuditHeaders.getLogName(), "host=" + host,
              "stage=" + stage.toString());
    }
  }

  /**
   *  add audit config for some topic. config is constructed from the properties.
   */
  public AuditConfig addAuditConfigFromMap(String loggingAuditName,
                                           Map<String, String> properties) {
    try {
      AuditConfig auditConfig = ConfigUtils.createAuditConfigFromMap(properties);
      return addAuditConfig(loggingAuditName, auditConfig);
    } catch (ConfigurationException e) {
      LOG.error("[{}] couldn't create TopicAuditConfig for {}.", Thread.currentThread().getName(),
          loggingAuditName);
      return null;
    }
  }

  /**
   *  add audit config for some topic
   */
  public AuditConfig addAuditConfig(String loggingAuditName, AuditConfig auditConfig) {
    if (auditConfig != null) {
      this.auditConfigs.put(loggingAuditName, auditConfig);
      LOG.info("[{}] add TopicAuditConfig ({}) for {}.", Thread.currentThread().getName(),
          auditConfig.toString(), loggingAuditName);
    }
    return auditConfig;
  }

  /**
   *  check if there exists audit config for a topic / loggingAuditName
   */
  public boolean checkAuditConfigExists(String loggingAuditName) {
    return this.auditConfigs.containsKey(loggingAuditName);
  }

  public void close() {
    enqueueEnabled.set(false);
    LOG.info(
        "[{}] set enqueueEnabled to false and no more LoggingAudit events will be added to queue while closing LoggingAuditClient.",
        Thread.currentThread().getName());
    this.sender.stop();
    LOG.info("[{}] stopped the LoggingAuditSender.", Thread.currentThread().getName());
  }
}
