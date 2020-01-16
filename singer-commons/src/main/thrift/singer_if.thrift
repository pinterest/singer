// Log message definitions written by thrift logger and pick up by singer agent.

namespace py schemas.singer
namespace java com.pinterest.singer.thrift

include "loggingaudit.thrift"

/**
 * Log message that applications pass to logger.
 **/
struct LogMessage {
  // The optional message key.
  1: optional binary key;
  // The required message payload.
  2: required binary message;
  // The optional message timestamp in nano-seconds
  3: optional i64 timestampInNanos;
  // The CRC-32 checksum for the message
  4: optional i64 checksum;
  5: optional loggingaudit.LoggingAuditHeaders loggingAuditHeaders;
}


/**
 *  To guarantee data delivery over the pipeline, we need to track the number of messages
 *  that Singer delivers for each topic using a seperate Kafka topic.
 *
 *  Singer uses batch writing to write messages to Kafka. For each batch writing, we will
 *  have a message that writes to the audit topic on the number of log messages in the batch.
 *  Number of log messages + 1 (audit message) <= MAX_BATCH_SIZE
 **/
struct AuditMessage {
  // Timestamp in nano-seconds on when a batch of messages is going to be written to Kafka
  1: required i64 timestamp;
  // The hostname
  2: required string hostname;
  // The written topic name
  3: required string topic;
  // Number of messages that Singer's kafkaWriter writes to Kafka in one batch
  4: required i64 numMessages;
}
