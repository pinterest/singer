/* Configuration-related thrift messages */

namespace java com.pinterest.singer.thrift.configuration

include "common.thrift"
include "loggingaudit_config.thrift"

struct LogMonitorConfig {
  1: required i32 monitorIntervalInSecs;
}

struct LogStreamProcessorConfig {
  // Minimum processing interval.
  1: required i64 processingIntervalInMillisecondsMin;
  // Maximum processing interval.
  2: required i64 processingIntervalInMillisecondsMax;
  // Size of the batch for writing log messages.
  3: required i32 batchSize;
  // The maximum time in seconds for one processing cycle
  4: optional i64 processingTimeSliceInMilliseconds = 864000000;
}

enum ReaderType {
  THRIFT = 0,
  TEXT = 1
}

struct ThriftReaderConfig {
  1: required i32 readerBufferSize;
  2: required i32 maxMessageSize;
}

enum TextLogMessageType {
  THRIFT_TEXT_MESSAGE = 0,
  PLAIN_TEXT = 1
}

struct TextReaderConfig {
  // Reader buffer size.
  1: required i32 readerBufferSize;
  // Maximum text message size
  2: required i32 maxMessageSize;
  // Num of text messages packed into one Singer log message
  3: required i32 numMessagesPerLogMessage;
  // Regex used to detect the start line of a message.
  // For files where each line is a text message, this should be "^".
  // For files where text message can across lines, this should be something like:
  // "^[IWF][0-9]{5}" which match a line start with any char from "IWF" and followed by 5 digits.
  4: required string messageStartRegex;
  // the log message format, can be TextMessage, or String
  5: optional TextLogMessageType textLogMessageType = 0;
  // whether the text reader prepends timestamp in the message
  6: optional bool prependTimestamp = false;
  // whehter the text reader prepends hostname in the message
  7: optional bool prependHostname = false;
  // the delimiter for prepended messages
  8: optional string prependFieldDelimiter = " ";
}

struct LogStreamReaderConfig {
  1: required ReaderType type;
  2: optional ThriftReaderConfig thriftReaderConfig;
  3: optional TextReaderConfig textReaderConfig;
}

/**
 * KAFKA:  kafka writer that distributes messages to topic partitions based on
 *         partititioner. The default partitioner does random distribution among
 *         all partitions of a topic. Using a writer threadpool underneath, we can
 *         achieve maximum Kafka writing throughput using this writer.
 *
 * DUMMY:  DummyLogStreamWriter ignores the log messages and does not do actually writing.
 *
 **/
enum WriterType {
  DUMMY = 0,
  KAFKA08 = 1,
  KAFKA = 2,
  REALPIN = 3,
  PULSAR = 4
}

enum FileNameMatchMode {
  EXACT = 0,
  PREFIX = 1
}

struct KafkaWriterConfig {
  1: required string topic;
  2: required common.KafkaProducerConfig producerConfig;
  3: optional string auditTopic;
  4: optional bool auditingEnabled = 0;
  5: optional bool skipNoLeaderPartitions = 0;
  6: optional i32 writeTimeoutInSeconds = 60;
}

struct DummyWriteConfig {
  1: required string topic;
}

enum RealpinObjectType {
  MOBILE_PERF_LOG = 0
  PIN_PROMOTIONS_INSERTION = 1
}

struct RealpinWriterConfig {
  1: required string topic;
  2: required RealpinObjectType objectType;
  3: required string serverSetPath;
  4: optional i32 timeoutMs = 1000;
  5: optional i32 retries = 3;
  6: optional i32 hostLimit = 200;
  7: optional i32 maxWaiters = 200;
  8: optional i32 ttl = -1;
}

struct PulsarProducerConfig {
  1: required string pulsarClusterSignature;
  2: required string serviceUrl;
  3: optional string compressionType = "NONE";
  4: optional i32 writeTimeoutInSeconds = 60;
  5: optional string partitionerClass = "com.pinterest.singer.writer.pulsar.DefaultPartitioner";
  6: optional i32 batchingMaxMessages = 100;
  7: optional i32 maxPendingMessages = 2000;
  8: optional i32 batchingMaxPublishDelayInMilli = 10;
  9: optional i32 maxPendingMessagesAcrossPartitions = 3000;
}

struct PulsarWriterConfig {
  1: required string topic;
  2: required PulsarProducerConfig producerConfig;
  3: optional string auditTopic;
  4: optional bool auditingEnabled = 0;
}

struct LogStreamWriterConfig {
  1: required WriterType type;
  2: optional KafkaWriterConfig kafkaWriterConfig;
  3: optional DummyWriteConfig dummyWriteConfig;
  4: optional RealpinWriterConfig realpinWriterConfig;
  5: optional PulsarWriterConfig pulsarWriterConfig;
}

struct HeartbeatWriterConfig {
  1: required WriterType type;
  2: optional KafkaWriterConfig kafkaWriterConfig;
}

struct SingerLogConfig {
  1: required string name;
  2: required string logDir;
  3: required string logStreamRegex;
  4: required LogStreamProcessorConfig logStreamProcessorConfig;
  5: required LogStreamReaderConfig logStreamReaderConfig;
  6: required LogStreamWriterConfig logStreamWriterConfig;
  7: optional string logDecider;
  8: optional FileNameMatchMode filenameMatchMode;
  /**
   * the maximum retention of a log file based on its last modification time.
   * Default value is -1 that means that there is no log retention enforcement by Singer.
   */
  9: optional i32 logRetentionInSeconds = -1;
  10: optional bool enableHeadersInjector = false;
  /**
   * headers injector class
   */
  11: optional string headersInjectorClass = "com.pinterest.singer.writer.headersinjectors.LoggingAuditHeadersInjector";

  12: optional bool enableLoggingAudit = false;
  /**
   *  audit configuration for the logStreams created from this SingerLogConfig
   */
  13: optional loggingaudit_config.AuditConfig auditConfig;
}

/**
 *  The configuration for defining the auto-restart settings for singer.
 *  We have two different kinds of restart:
 *
 *  1. restart itself in case of too-many log uploading failures
 *
 *  2. daily restart for cleaning up
 **/
struct SingerRestartConfig {
  1: optional bool restartOnFailures = 0;
  2: optional i32 numOfFailuesAllowed = 100;

  3: optional bool restartDaily = 0;
  4: optional string dailyRestartUtcTimeRangeBegin = "02:30"; // HH:MM
  5: optional string dailyRestartUtcTimeRangeEnd = "03:30" ;  // HH:MM
}

struct KubeConfig {

  /**
   * Poll frequency for kubelet metadata
   */
  1: optional i32 pollFrequencyInSeconds = 10;
  
  /**
   * Location of pod log mounts on kubelet
   */
  2: optional string podLogDirectory = "";
   
  /**
   * Enable if running singer on kubernetes
   */
  3: optional i32 defaultDeletionTimeoutInSeconds = 3600;
  
  /**
   * Frequency to check if all pod logs have been processed
   */
  4: optional i32 deletionCheckIntervalInSeconds = 20;
  
  /**
   * Kube Poll start delay; this is to prevent pod deletion 
   * events from being triggered too quickly
   */
  5: optional i32 kubePollStartDelaySeconds = 10; 

}

/**
 * The singer config, synthesized from both the singer's own config and possible user config files.
 */
struct SingerConfig {
  /**
   * global thread pool size. Parsed directly from the singer config file.
   */
  1: required i32 threadPoolSize = 20;

  /**
   * port number of ostrich. Parsed directly from the singer config file.
   */
  2: required i32 ostrichPort = 2047;

  /**
   * log monitor parameters. Parsed directly from the singer config file.
   */
  3: required LogMonitorConfig logMonitorConfig;

  /**
   *  Synthesized application log config  from user config files.
   */
  4: required list<SingerLogConfig> logConfigs;

  /**
   * The interval in seconds of Singer's poll the config directory.
   */
  5: optional i32 logConfigPollIntervalSecs = 10;

  /**
   * Stats pusher host and port
   */
  6: optional string statsPusherHostPort;

  /**
   * the size of thread pool for writing logs to central storage
   */
  7: optional i32 writerThreadPoolSize = 16;

  /**
   * whether heartbeat is enabled for the singer instance
   */
  8: optional bool heartbeatEnabled = true;

  /**
   *  heartbeat interview in seconds
   */
  9: optional i32 heartbeatIntervalInSeconds = 60;

  /**
  *  heartbeat writer config
  */
  10: optional HeartbeatWriterConfig heartbeatWriterConfig;

  /**
   * the configurations for restarting singer
   */
  11: optional SingerRestartConfig singerRestartConfig;

  /**
   * the time to wait for a file rotation to finish
   */
  12: optional i32 logFileRotationTimeInMillis = 200;

  /**
   * logmonitor setting
   */
  13: optional string logMonitorClass = "com.pinterest.singer.monitor.DefaultLogMonitor";
 
  /**
   * if running on kubernetes host
   */
  14: optional bool kubernetesEnabled = false;
 
  /**
   * Kubernetes config
   */ 
  15: optional KubeConfig kubeConfig;
  
  /**
   * Active or deactivate command server
   */
  16: optional bool runCommandServer = false;
  
  /** 
   * Class name for stats pusher
   */
  17: optional string statsPusherClass = "com.pinterest.singer.metrics.OpenTsdbStatsPusher";
  
  /**
   * Singer Environment provider class
   */
  18: optional string environmentProviderClass;

  19: optional bool loggingAuditEnabled = false;

  20: optional loggingaudit_config.LoggingAuditClientConfig loggingAuditClientConfig;

}