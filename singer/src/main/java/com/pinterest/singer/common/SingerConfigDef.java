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
package com.pinterest.singer.common;

import org.apache.kafka.clients.producer.ProducerConfig;

public class SingerConfigDef {

  public static final String SINGER_CONFIGURATION_PREFIX = "singer.";
  public static final String AUDIT_TOPIC = "auditTopic";
  public static final String AUDITING_ENABLED = "auditingEnabled";
  public static final String COMPRESSION_TYPE = ProducerConfig.COMPRESSION_TYPE_CONFIG;

  public static final String SINGER_RESTART_PREFIX = "singer.restart.";
  public static final String ON_FAILURES = "onFailures";
  public static final String DAILY_RESTART_FLAG = "daily";
  public static final String NUMBER_OF_FAILURES_ALLOWED = "numberOfFailuresAllowed";
  public static final String DAILY_RESTART_TIME_BEGIN = "dailyRestartUtcTimeRangeBegin";
  public static final String DAILY_RESTART_TIME_END = "dailyRestartUtcTimeRangeEnd";

  public static final String MONITOR_INTERVAL_IN_SECS = "monitorIntervalInSecs";

  public static final String PROCESS_INTERVAL_SECS = "processingIntervalInSeconds";
  public static final String PROCESS_INTERVAL_MILLIS = "processingIntervalInMilliseconds";

  public static final String PROCESS_INTERVAL_SECS_MAX = "processingIntervalInSecondsMax";
  public static final String PROCESS_INTERVAL_MILLIS_MAX = "processingIntervalInMillisecondsMax";

  public static final String PROCESS_TIME_SLICE_MILLIS = "processingTimeSliceInMilliseconds";
  public static final String PROCESS_TIME_SLICE_SECS = "processingTimeSliceInSeconds";
  public static final String PRODUCER_CONFIG_PREFIX = "producerConfig.";
  public static final String SKIP_NO_LEADER_PARTITIONS = "skipNoLeaderPartitions";
  public static final String TOPIC = "topic";

  public static final String KAFKA_WRITE_TIMEOUT_IN_SECONDS = "writeTimeoutInSeconds";
  public static final String LOG_RETENTION_SECONDS = "logRetentionInSeconds";

  public static final String PRODUCER_BUFFER_MEMORY = ProducerConfig.BUFFER_MEMORY_CONFIG;
  public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
  public static final String BOOTSTRAP_SERVERS_FILE = "bootstrap.servers.file";
  @Deprecated
  public static final String BROKER_SERVERSET = "broker.serverset";
  @Deprecated
  public static final String BROKER_SERVERSET_DEPRECATED = "metadata.broker.serverset";
  public static final String ACKS = "acks";

  public static final String REALPIN_OBJECT_TYPE = "objectType";
  public static final String REALPIN_SERVERSET_PATH = "serverSetPath";
  public static final String REALPIN_TIMEOUT_MS = "timeoutMs";
  public static final String REALPIN_RETRIES = "retries";
  public static final String REALPIN_HOST_LIMIT = "hostLimit";
  public static final String REALPIN_MAX_WAITERS = "maxWaiters";
  public static final String REALPIN_TTL = "ttl";

  public static final String TOPIC_NAMES = "topic_names";
  public static final String DEFAULT_PARTITIONER = "com.pinterest.singer.writer.partitioners.DefaultPartitioner";

  public static final String SINGER_KUBE_CONFIG_PREFIX = "singer.kubernetes.";
  public static final String KUBE_POLL_FREQUENCY_SECONDS = "pollFrequencyInSeconds";
  public static final String KUBE_POD_LOG_DIR = "podLogDirectory";
  public static final String KUBE_DEFAULT_DELETION_TIMEOUT = "defaultDeletionTimeoutInSeconds";
  
  public static final int SINGER_EXIT_FSM_EXCEPTION = 200;
  public static final int SINGER_EXIT_FSEF_EXCEPTION = 201;

  public static final String SSL_ENABLED_CONFIG = "ssl.enabled";
  public static final String TRANSACTION_ENABLED_CONFIG = "transaction.enabled";
  public static final String TRANSACTION_TIMEOUT_MS_CONFIG = "transaction.timeout.ms";
  public static final String RETRIES_CONFIG = "retries";
  
  public static final String PARTITIONER_CLASS = "partitioner.class";
  public static final String REQUEST_REQUIRED_ACKS = "request.required.acks";
  public static final String PULSAR_SERVICE_URL = "pulsarServiceUrl";
  
}