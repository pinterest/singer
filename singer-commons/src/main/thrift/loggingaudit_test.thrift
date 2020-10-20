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

namespace py schemas.loggingaudit
namespace java com.pinterest.singer.loggingaudit.thrift
include "loggingaudit.thrift"

/**
 *  AuditDemoLog1Message is used to generate thrift objects for testing.
 **/
struct  AuditDemoLog1Message {
  1: required i64 timestamp;
  2: required i64 seqId; // starts from 0
  3: optional binary payload;
  4: optional loggingaudit.LoggingAuditHeaders loggingAuditHeaders;
}
