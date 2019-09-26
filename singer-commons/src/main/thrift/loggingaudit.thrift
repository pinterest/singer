// Log message definitions written by thrift logger and pick up by singer agent.

namespace py schemas.loggingaudit
namespace java com.pinterest.singer.loggingaudit.thrift

/**
 *  Common headers added to original thrift message (thrift object) as well as to LogMessage
 *  which is the format ThriftLogger writes data to files.
 *
 *  These common headers together can uniquely identify each log message and provide information
 *  for auditing or tracing use case.
 **/

struct LoggingAuditHeaders {
   /**
    *  Host on which log messages are generated.
    */
   1: required string host;

   /**
    *  Topic that log message will be sent to eventually.
    */
   2: required string topic;

   /**
    *  Process that generate log message.
    */
   3: required i32 pid;

   /**
    *  Timestamp when ThriftLogger instance is initialized.
    */
   4: required i64 session;

   /**
    *  Log sequence number for a given session and it starts with 0.
    */
   5: required i32 logSeqNumInSession;
}

