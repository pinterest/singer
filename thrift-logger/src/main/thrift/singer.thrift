// Internal thrift message used in Singer

namespace java com.pinterest.singer.thrift
namespace py singer

include "singer_if.thrift"

struct LogFile {
  1: required i64 inode;
}

struct LogFileAndPath {
  // LogFile
  1: required LogFile logFile;

  // LogFile's path in filesystem.
  2: required string path;
}

struct LogPosition {
  // The LogFile to which the byte offset is relative to.
  1: required LogFile logFile;

  // Number of bytes relative to the LogFile's beginning.
  2: required i64 byteOffset;
}

struct LogMessageAndPosition {
  // Log message.
  1: required singer_if.LogMessage logMessage;

  // The position which points to the next byte after this LogMessage.
  // Note this is NOT the byte offset of this LogMessage.
  2: required LogPosition nextPosition;
}
