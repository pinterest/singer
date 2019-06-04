// Text log message from text log file

namespace java com.pinterest.singer.thrift
namespace py schemas.text_message

struct TextMessage {
  1: required list<string> messages;
  2: optional string host;
  3: optional string filename;
}
