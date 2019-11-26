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

public class LoggingAuditClientMetrics {

  public static final String AUDIT_CLIENT_QUEUE_SIZE = "audit.client.queue.size";
  public static final String AUDIT_CLIENT_QUEUE_USAGE_PERCENT = "audit.client.queue.usage_percent";
  public static final String AUDIT_CLIENT_ENQUEUE_EXCEPTION = "audit.client.enqueue.exception";
  // this metric tracks the number of audit events dropped because enqueue fails.
  public static final String AUDIT_CLIENT_ENQUEUE_EVENTS_DROPPED= "audit.client.enqueue.events_dropped";
  public static final String AUDIT_CLIENT_ENQUEUE_EVENTS_ADDED = "audit.client.enqueue.events_added";

  public static final String AUDIT_EVENT_WITHOUT_TOPIC_CONFIGURED_ERROR = "audit.event.without_topic_configured.error";

  // this metric tracks the number of audit events dropped when enqueue is disabled after calling stop() method of LoggingAuditClient.
  // If the shutdown hook or cleanup logic is implemented correctly at any LoggingAuditStage, the value of this metric should be zero.
  public static final String AUDIT_CLIENT_ENQUEUE_DISABLED_EVENTS_DROPPED= "audit.client.enqueue.disabled.events_dropped";

  public static final String AUDIT_CLIENT_SENDER_DEQUEUE_INTERRUPTED_EXCEPTION= "audit.client.sender.dequeue_interrupted_exception";
  public static final String AUDIT_CLIENT_SENDER_EXCEPTION = "audit.client.sender.exception";

  public static final String AUDIT_CLIENT_SENDER_SERIALIZATION_EXCEPTION = "audit.client.sender.serialization.exception";
  public static final String AUDIT_CLIENT_SENDER_KAFKA_PARTITION_ERROR = "audit.client.sender.kafka.partition_errors";
  public static final String AUDIT_CLIENT_SENDER_KAFKA_EVENTS_DROPPED= "audit.client.sender.kafka.events_dropped";
  public static final String AUDIT_CLIENT_SENDER_KAFKA_EVENTS_ACKED= "audit.client.sender.kafka.events_acked";
  public static final String AUDIT_CLIENT_SENDER_KAFKA_EVENTS_RETRIED = "audit.client.sender.kafka.events_retried";

  public static final String  AUDIT_CLIENT_SENDER_KAFKA_PARTITIONS_REFRESH_ERROR = "audit.client.sender.kafka.partitions_refresh_error";
  public static final String  AUDIT_CLIENT_SENDER_KAFKA_PARTITIONS_REFRESH_COUNT = "audit.client.sender.kafka.partitions_refresh_count";

  public static final String AUDIT_CLIENT_SENDER_KAFKA_CALLBACK_EXCEPTION = "audit.client.sender.kafka.callback_exception";
}
