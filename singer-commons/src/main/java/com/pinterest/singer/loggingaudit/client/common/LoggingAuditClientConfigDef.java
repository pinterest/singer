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

package com.pinterest.singer.loggingaudit.client.common;

import org.apache.kafka.clients.producer.ProducerConfig;

public class LoggingAuditClientConfigDef {

  public static final String STAGE = "stage";
  public static final String DEFAULT_ENABLE_AUDIT_FOR_ALL_TOPICS = "enableAuditForAllTopicsByDefault";
  public static final String QUEUE_SIZE = "queueSize";
  public static final String ENQUEUE_WAIT_IN_MILLISECONDS = "enqueueWaitInMilliseconds";

  public static final String AUDITED_TOPICS_PREFIX = "auditedTopics.";
  public static final String AUDITED_TOPIC_NAMES = "names";
  public static final String SAMPLING_RATE = "samplingRate";
  public static final String START_AT_CURRENT_STAGE = "startAtCurrentStage";
  public static final String STOP_AT_CURRENT_STAGE = "stopAtCurrentStage";


  public static final String SENDER_PREFIX = "sender.";
  public static final String SENDER_TYPE = "type";

  public static final String KAFKA_SENDER_PREFIX = "kafka.";
  public static final String KAFKA_TOPIC = "topic";
  public static final String KAFKA_STOP_GRACE_PERIOD_IN_SECONDS = "stopGracePeriodInSeconds";

  public static final String KAFKA_PRODUCER_CONFIG_PREFIX = "producerConfig.";
  public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
  public static final String SSL_ENABLED_CONFIG = "ssl.enabled";
  public static final String TRANSACTION_ENABLED_CONFIG = "transaction.enabled";
  public static final String TRANSACTION_TIMEOUT_MS_CONFIG = "transaction.timeout.ms";
  public static final String RETRIES_CONFIG = "retries";
  public static final String COMPRESSION_TYPE = ProducerConfig.COMPRESSION_TYPE_CONFIG;
  public static final String ACKS = "acks";
  public static final String DEFAULT_ACKS = "-1";
  public static final String SECURE_KAFKA_PRODUCER_CONFIG_PREFIX = "ssl";

}
