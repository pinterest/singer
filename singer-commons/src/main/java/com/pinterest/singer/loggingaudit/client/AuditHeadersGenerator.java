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
package com.pinterest.singer.loggingaudit.client;

import com.pinterest.singer.utils.CommonUtils;
import com.pinterest.singer.loggingaudit.thrift.LoggingAuditHeaders;

/**
 *  This class is used to generate LoggingAuditHeaders object which is defined as thrift struct.
 *  LoggingAuditHeaders object can uniquely identify each log message and support auditing use case.
 *
 *  Each private of AuditHeadersGenerator class corresponds to each field of LoggingAuditHeaders.
 **/

public class AuditHeadersGenerator {

  /**
   *  Host on which log messages are generated.
   */
  private String host;

  /**
   *  name of log files where the log messages are written to.
   */
  private String logName;

  /**
   *  Process that generate log message.
   */
  private int pid;

  /**
   *  Timestamp when ThriftLogger instance is initialized.
   */
  private long session;

  /**
   *  Log sequence number for a given session and it starts with 0.
   */
  private int logSeqNumInSession;

  public AuditHeadersGenerator(String host, String logName) {
    this.host = host;
    this.logName = logName;
    this.pid = CommonUtils.getPid();
    this.session = System.currentTimeMillis();
    this.logSeqNumInSession = -1;
  }

  public LoggingAuditHeaders generateHeaders() {
    if (this.logSeqNumInSession == Integer.MAX_VALUE) {
      this.session = System.currentTimeMillis();
      this.logSeqNumInSession = -1;
    }
    this.logSeqNumInSession += 1;
    return new LoggingAuditHeaders()
        .setHost(this.host)
        .setLogName(this.logName)
        .setPid(this.pid)
        .setSession(this.session)
        .setLogSeqNumInSession(this.logSeqNumInSession)
        .setTimestamp(System.currentTimeMillis());
  }

}
