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

import com.twitter.ostrich.stats.Stats;

/**
 *  This class keeps the names of all Singer stats.
 */
public class SingerMetrics {

  private static final String SINGER_PREIX = "singer.";
  private static final String SINGER_FSM_PREFIX = SINGER_PREIX + "file_system_monitor.";
  public static final String SINGER_WRITER = SINGER_PREIX + "writer.";

  public static final String IO_EXCEPTION_INVALID_DIR = SINGER_FSM_PREFIX + "invalid_dir";
  public static final String IO_EXCEPTION_METRIC_NAME = SINGER_FSM_PREFIX + "ioexception";
  public static final String FSEF_EXCEPTION           = SINGER_FSM_PREFIX + ".eventfetcher.exception" ;
  public static final String FS_EVENT                 = SINGER_FSM_PREFIX + "event";
  public static final String FS_EVENTS_OVERFLOW       = SINGER_FSM_PREFIX + "events_overflow";
  public static final String FS_EVENT_QUEUE_SIZE      = SINGER_FSM_PREFIX + "queue_size";
  public static final String MISSING_LOG_FILES        = SINGER_FSM_PREFIX + "missing_log_files";

  public static final String FILE_LOOKUP_SUCCESS = "singer.file_lookup.success";
  public static final String FILE_LOOKUP_FAILURE = "singer.file_lookup.failure";
  
  public static final String SINGER_START_INDICATOR = "singer.starting";

  public static final String NO_SUCH_FILE_EXCEPTION = "singer.singer_log.nosuchfileexception";

  public static final String LOGSTREAM_CREATION_FAILURE = "singer.logstream.creation_failure";

  public static final String LOGSTREAM_INITIALIZE = "singer.logstream.initialize";

  public static final String LOGSTREAM_FILE_DELETION = "singer.logstream.file_deletion";

  public static final String NUM_LOGSTREAMS = "singer.monitor.num_logstreams_processed";

  public static final String NUM_STUCK_LOGSTREAMS = "singer.processor.stuck_processors";

  public static final String PROCESSOR_EXCEPTION = "singer.processor.exception";

  public static final String PROCESSOR_LATENCY = "processor.latency";

  public static final String CURRENT_PROCESSOR_LATENCY = "current.processor.latency";

  public static final String SKIPPED_BYTES = "singer.reader.skipped_bytes";

  public static final String WATERMARK_CREATION_FAILURE = "singer.watermark.creation.failure";

  public static final String WATERMARK_RENAME_FAILURE = "singer.watermark.rename.failure";

  public static final String NUM_KAFKA_MESSAGES = SINGER_WRITER + "num_kafka_messages_delivery_success";
  public static final String OVERSIZED_MESSAGES = SINGER_WRITER + "num_oversized_messages";
  public static final String WRITE_FAILURE        = SINGER_WRITER + "kafka_write_failure";
  public static final String BROKER_WRITE_FAILURE        = SINGER_WRITER + "broker_write_failure";
  public static final String BROKER_WRITE_SUCCESS        = SINGER_WRITER + "broker_write_success";
  public static final String BROKER_WRITE_LATENCY        = SINGER_WRITER + "broker_write_latency";
  public static final String WRITER_BATCH_SIZE    = SINGER_WRITER + "message_batch_size";
  public static final String WRITER_SSL_EXCEPTION = SINGER_WRITER + "ssl_exception";
  public static final String KAFKA_THROUGHPUT = SINGER_WRITER + "topic_kafka_throughput";
  public static final String KAFKA_LATENCY = SINGER_WRITER + "max_kafka_batch_write_latency";
  public static final String NUM_COMMITED_TRANSACTIONS = SINGER_WRITER + "num_committed_transactions";
  public static final String NUM_ABORTED_TRANSACTIONS = SINGER_WRITER + "num_aborted_transactions";
  public static final String NUM_KAFKA_PRODUCERS = SINGER_WRITER + "num_kafka_producers";

  public static final String KUBE_PREFIX           = SINGER_PREIX + "kube.";
  public static final String KUBE_API_ERROR        = KUBE_PREFIX  + "api_error";
  public static final String ACTIVE_POD_DELETION_TASKS = KUBE_PREFIX + "pod_deletion_tasks_active";
  public static final String PODS_TOMBSTONE_MARKER = KUBE_PREFIX + "pod_tombstone_marker";
  public static final String PODS_DELETED = KUBE_PREFIX + "pod_deleted";
  public static final String PODS_CREATED = KUBE_PREFIX + "pod_created";
  // Time elapsed between when Kubernetes deleted the pod and when Singer wrote tombstone marker
  public static final String POD_DELETION_TIME_ELAPSED = KUBE_PREFIX + "pod_deletion_time_elapsed";
  public static final String NUMBER_OF_PODS = KUBE_PREFIX + "number_of_pods";
  
  static {
    // note this needs to be initialized since we update this
    // gauge based on current value
    Stats.setGauge(SingerMetrics.POD_DELETION_TIME_ELAPSED, 0);
  }

  public static final String SINGER_CONFIGURATOR_CONFIG_ERRORS = "singer.configurator.config_errors";
  public static final String SINGER_CONFIGURATOR_CONFIG_ERRORS_UNKNOWN = "singer.configurator.unexpected_config_errors";
  public static final String LOCALITY_MISSING = "singer.locality.missing";
  public static final String DISABLE_SINGER_OSTRICH = "DISABLE_OSTRICH_METRICS";
  public static final String LEADER_INFO_EXCEPTION = SINGER_WRITER + "leader_info_exception";
  public static final String MISSING_LOCAL_PARTITIONS = "singer.locality.missing_local_partitions";
  public static final String MISSING_DIR_CHECKER_INTERRUPTED = "singer.missing_dir_checker.thread_interrupted";
  public static final String NUMBER_OF_MISSING_DIRS = "singer.missing_dir_checker.num_of_missing_dirs";
  public static final String NUMBER_OF_SERIALIZING_HEADERS_ERRORS = "singer.headers_injector.num_of_serializing_headers_errors";
  public static final String AUDIT_HEADERS_INJECTED = "singer.audit.num_of_headers_injected";
  public static final String AUDIT_HEADERS_METADATA_COUNT_MISMATCH = "singer.audit.headers_metadata_count_mismatch";
  public static final String AUDIT_HEADERS_METADATA_COUNT_MATCH = "singer.audit.headers_metadata_count_match";
  public static final String NUM_CORRUPTED_MESSAGES = "singer.audit.num_corrupted_messages";
  public static final String NUM_CORRUPTED_MESSAGES_SKIPPED = "singer.audit.num_corrupted_messages_skipped";


  // Used when logging audit is enabled and is started at Singer instead of ThriftLogger.
  public static final String AUDIT_HEADERS_SET_FOR_LOG_MESSAGE = "singer.audit.log_message_headers_set";
  public static final String AUDIT_HEADERS_SET_FOR_LOG_MESSAGE_EXCEPTION = "singer.audit.log_message_headers_set.exception";
  public static final String SHADOW_MODE_ENABLED = "singer.shadow_mode_enabled";
}