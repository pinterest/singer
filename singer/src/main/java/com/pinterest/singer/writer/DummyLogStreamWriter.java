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
package com.pinterest.singer.writer;

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.LogStreamWriter;
import com.pinterest.singer.common.errors.LogStreamWriterException;
import com.pinterest.singer.thrift.LogMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Dummy writer used for Singer testing
 */
public class DummyLogStreamWriter implements LogStreamWriter {

  private static final Logger LOG = LoggerFactory.getLogger(DummyLogStreamWriter.class);

  private LogStream logStream;
  private String topic;

  public DummyLogStreamWriter(LogStream logStream, String topic) {
    this.logStream = logStream;
    this.topic = topic;
  }

  public DummyLogStreamWriter(LogStream logStream) {
    this.logStream = logStream;
    this.topic = null;
  }

  @Override
  public LogStream getLogStream() {
    return this.logStream;
  }

  @Override
  public boolean isAuditingEnabled() {
    return false;
  }

  @Override
  public void writeLogMessages(List<LogMessage> messages) throws LogStreamWriterException {
    LOG.info("Wrote {} messages with total size {} bytes for topic {} successfully.", messages.size(), topic, messages.stream().mapToInt(m -> m.getMessage().length).sum());
  }

  @Override
  public void close() throws IOException {
    LOG.info("writer closed");
  }
}