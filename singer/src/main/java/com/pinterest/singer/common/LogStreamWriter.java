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

import com.pinterest.singer.common.errors.LogStreamWriterException;
import com.pinterest.singer.thrift.LogMessage;

import java.io.Closeable;
import java.util.List;

/**
 * Represent a writer that writes LogMessages from a LogStream to a destination.
 */
public interface LogStreamWriter extends Closeable {

  /**
   * @return the LogStream this LogStreamWriter is associated with.
   */
  LogStream getLogStream();

  /**
   * @return whether auditing is enabled for the log stream writer
   */
  boolean isAuditingEnabled();

  /**
   * Write a sequence of LogMessages to some destination.
   *
   * @param messages The LogMessages to be written.
   * @throws LogStreamWriterException when writer fails to write the LogMessages.
   */
  void writeLogMessages(List<LogMessage> messages) throws LogStreamWriterException;
}
