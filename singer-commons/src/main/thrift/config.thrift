/* Configuration-related thrift messages */

namespace java com.pinterest.singer.thrift.configuration

include "common.thrift"
include "loggingaudit_config.thrift"

struct LogMonitorConfig {
  1: required i32 monitorIntervalInSecs;
}

enum SamplingType {
    NONE = 0,
    MESSAGE = 1,
    INSTANCE = 2
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
  // Enable memory efficient processor
  5: optional bool enableMemoryEfficientProcessor = true;
  // (DEPRECATED) Enable decider based sampling
  6: optional bool enableDeciderBasedSampling = false;
  // Sampling type
  7: optional SamplingType deciderBasedSampling = 0;
}

enum ReaderType {
  THRIFT = 0,
  TEXT = 1
}

struct ThriftReaderConfig {
  1: required i32 readerBufferSize;
  2: required i32 maxMessageSize;
  // custom environment variables to be injected into thrift logs
  3: optional map<string, binary> environmentVariables;
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
  // ability to trim trailing new line character
  9: optional bool trimTailingNewlineCharacter = false;
  // custom environment variables to be injected into text logs
  10: optional map<string, binary> environmentVariables;
  // Regex used to filter out messaqges that do not match the regex.
  11: optional string filterMessageRegex;
}

struct LogStreamReaderConfig {
  1: required ReaderType type;
  2: optional ThriftReaderConfig thriftReaderConfig;
  3: optional TextReaderConfig textReaderConfig;
}

/**
  * Transforms related configurations
  *
  * REGEX_BASED_MODIFIER:  A regex based modifier that modifies the log message based on the regex
  *                        and the modified message format.
 **/
enum TransformType {
  REGEX_BASED_MODIFIER = 0
}

struct RegexBasedModifierConfig {
  // The regex to match the log message.
  1: required string regex;
  // The modified message format. The regex captured groups can be referenced by $1, $2, etc.
  2: required string modifiedMessageFormat;
  // The encoding of the log message.
  3: optional string encoding = "UTF-8";
  // Append a newline to the end of the modified message.
  4: optional bool appendNewLine = true;
}

struct MessageTransformerConfig {
  1: required TransformType type;
  2: optional RegexBasedModifierConfig regexBasedModifierConfig;
}

/**
 * KAFKA:  kafka writer that distributes messages to topic partitions based on
 *         partititioner. The default partitioner does random distribution among
 *         all partitions of a topic. Using a writer threadpool underneath, we can
 *         achieve maximum Kafka writing throughput using this writer.
 *
 * NO_OP:  NoOpLogStreamWriter ignores the log messages and does not do actually writing.
 *
 **/
enum WriterType {
  NO_OP = 0,
  KAFKA08 = 1,
  KAFKA = 2,
  REALPIN = 3,
  PULSAR = 4,
  MEMQ = 5,
  S3 = 6
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

struct NoOpWriteConfig {
  1: required string topic;
}

struct S3WriterConfig {
  // The S3 bucket to upload the log files to.
  1: required string bucket;
  // The format of the key to be used for the S3 object. The key can contain tokens that will be replaced
  // with the values extracted from the log filename based on the regex pattern provided in filenamePattern.
  // e.g. "%{namespace}/%{service}/my_log.%UUID".
  2: required string keyFormat;
  // Max file size in MB. If the file size exceeds this value, it will be uploaded to S3.
  3: optional i32 maxFileSizeMB = 50;
  // Min upload time in seconds. If the file size exceeds this value, it will be uploaded to S3.
  4: optional i32 minUploadTimeInSeconds = 30;
  // The maximum number of retries for uploading a file to S3 before giving up.
  // Retry mechanism uses exponential backoff.
  5: optional i32 maxRetries = 5;
  // The directory where the log messages will be buffered before triggering
  // the upload to S3. The directory will be created if it does not exist.
  6: optional string bufferDir = "/tmp/singer/s3";
  // The regex pattern used to extract specific fields from the filename.
  // e.g. "^(?<service>[a-zA-Z0-9]+)_.*_(?<index>\\d+)\\.log$" will extract the "service" and "1000"
  // tokens from the filename "service_env_1000.log".
  7: optional string filenamePattern;
  // A comma separated list of named capturing groups that will be extracted from the filename based on the regex pattern provided in filenamePattern.
  // The extracted fields will be used to replace the tokens in keyFormat, e.g "namespace,service".
  8: optional list<string> filenameTokens;
  // The S3 canned access control lists (ACLs) that can be applied to uploaded objects to determine their access permissions.
  // We don't set a default since some buckets don't allow setting canned ACLs.
  9: optional string cannedAcl;
  // The uploader class to use for uploading objects to S3.
  10: optional string uploaderClass = "com.pinterest.singer.writer.s3.PutObjectUploader";
  // Region of the S3 bucket.
  11: optional string region = "us-east-1";
  // Use absolute path for filenamePattern regex matching.
  12: optional bool matchAbsolutePath = false;
  // The content type header to set for uploaded S3 objects.
  // e.g. "application/json", "text/plain", etc.
  13: optional string contentType;
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

struct MemqAuditorConfig {
  1: required string auditorClass;
  2: required string topic;
  3: optional string serverset;
}

struct MemqWriterConfig {
  1: required string serverset;
  2: required string topic;
  3: optional i32 maxInFlightRequests = 1;
  4: optional bool auditingEnabled = 0;
  5: optional i32 maxPayLoadBytes = 1048576;
  6: optional string compression = "GZIP";
  7: optional bool disableAcks = false;
  8: optional i32 ackCheckPollInterval = 500;
  9: optional string clientType = "HTTP";
  10: optional bool enableRackAwareness = true;
  11: required string cluster;
  12: optional MemqAuditorConfig auditorConfig;
  13: optional i32 maxInFlightRequestsMemoryBytes = 33554432;  // 32 MB
  14: optional i32 maxBlockMs = 0; // non-blocking by default
  15: optional i32 numWriteEndpoints = 1; // producer to broker mapping, 1:1 by default
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
  3: optional NoOpWriteConfig noOpWriteConfig;
  4: optional RealpinWriterConfig realpinWriterConfig;
  5: optional PulsarWriterConfig pulsarWriterConfig;
  6: optional MemqWriterConfig memqWriterConfig;
  7: optional S3WriterConfig s3WriterConfig;
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
  9: optional i32 logRetentionInSeconds = 7200;
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

  /**
   *  this log will be skipped directly when draining is enabled
   */
  14: optional bool skipDraining = false;

  /**
   *  Configuration to transform log message
   */
  15: optional MessageTransformerConfig messageTransformerConfig;
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

  /**
   * Directory that serves as a flag that indicates
   * a pod should be ingored
   */
  6: optional string ignorePodDirectory = "";

  /**
  * List of pod metadata fields to include per Singer log, Singer will not
  * track any pod metadata if this field is not set
  */
  7: optional list<string> podMetadataFields;

    /**
     * The path to the service account token file.
     * This is used to authenticate with the Kubernetes API server.
     */
    8: optional string serviceAccountTokenPath;

    /**
     * The path to the service account CA certificate file.
     * This is used to verify the Kubernetes API server's TLS certificate.
     */
    9: optional string serviceAccountCaCertPath;

    /**
     * The port number of the kubelet API server.
     * This is used to fetch pod metadata.
     */
    10: optional string kubeletPort = "10255";

    /**
     * Whether to use a secure connection to the kubelet API server.
     * If true, the serviceAccountTokenPath and serviceAccountCaCertPath must be set.
     */
    11: optional bool useSecureConnection = false;
}

struct AdminConfig {

  /**
   * socket file of the admin server
   */
  1: optional string socketFile = "/tmp/singer/admin.sock"

  /**
   * allowed uids
   */
  2: optional list<i64> allowedUids = [0]

  /**
  *
  */
  3: optional i32 defaultDeletionTimeoutInSeconds = 3600;

  /**
   *
   */
  4: optional i32 deletionCheckIntervalInSeconds = 20;

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
   * Class name for stats pusher
   */
  17: optional string statsPusherClass = "com.pinterest.singer.metrics.OpenTsdbStatsPusher";

  /**
   * Singer Environment provider class
   */
  18: optional string environmentProviderClass;

  19: optional bool loggingAuditEnabled = false;

  20: optional loggingaudit_config.LoggingAuditClientConfig loggingAuditClientConfig;

  /**
   * Run singer in shadow mode
   */
  21: optional bool shadowModeEnabled = false;

  /**
   * Run singer in shadow mode
   */
  22: optional string shadowModeServersetMappingFile;

  /**
   * Configure metrics pusher frequency
   */
  23: optional i32 statsPusherFrequencyInSeconds = 180;

  /**
   * Config override directory
   */

  24: optional string configOverrideDir;

  25: optional bool adminEnabled = false;

  /**
  *  Admin server configs
  */
  26: optional AdminConfig adminConfig;

  /**
    * FS Event Queue implementation
    */
  27: optional string fsEventQueueImplementation;

  /**
    * Hostname Prefix regex pattern
    */
  28: optional string hostnamePrefixRegex = "-";

  /**
  * Initialize LogStreamReaders with pooled buffers
  */
  29: optional bool enablePooledReaderBuffers = false;

}
