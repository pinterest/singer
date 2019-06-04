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
package com.pinterest.singer.client;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;

/**
 * ThriftLogger lets you log messages to Singer.
 *
 * Messages are written to disk per topic. Singer is responsible
 * for copying these to Kafka.
 */
public interface ThriftLogger {

  /**
   * Append a message to the given topic.
   * @param partitionKey an optional partition key.
   * @param message the message in bytes.
   * @param timeNanos timestamp of the message in nanoseconds.
   */
  void append(byte[] partitionKey, byte[] message, long timeNanos);

  /**
   * Append a thrift message to the given topic.
   * @param partitionKey an optional partition key.
   * @param thriftMessage the thrift object to log
   * @param timeNanos timestamp of the message in nanoseconds.
   */
  void append(byte[] partitionKey, TBase thriftMessage, long timeNanos) throws TException;

  /**
   * Append a thrift message to the given topic.
   * No partition key will be used, and the current wall-clock time will be logged
   * with the message.
   *
   * @param thriftMessage the thrift object to log
   */
  void append(TBase thriftMessage) throws TException;

  /**
   * Append a byte array to the given topic.
   * No partition key will be used, and the current wall-clock time will be logged
   * with the message.
   *
   * @param message the byte array to log
   */
  void append(byte[] message) throws TException;

  /**
   * Close the logger and flush and stop the underlying streams.
   */
  void close();
}
