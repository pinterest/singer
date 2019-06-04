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

import java.util.ArrayList;
import java.util.List;

/**
 * This mocks the behaviors of ThriftLogger for unit test purpose.
 * Call getMessages() to get the TBase messages that have been appended to logger.
 * Call getRawMessages() to get the byte[] messages that have been appended to logger.
 */
public class MockThriftLogger implements ThriftLogger {
  private static List<TBase> thriftMessages = new ArrayList<TBase>();
  private static List<byte[]> rawMessages = new ArrayList<byte[]>();

  @Override
  public void append(byte[] partitionKey, byte[] message, long timeNanos) {
    rawMessages.add(message);
  }

  @Override
  public void append(byte[] partitionKey, TBase thriftMessage, long timeNanos) throws TException {
    thriftMessages.add(thriftMessage);
  }

  @Override
  public void append(TBase thriftMessage) throws TException {
    thriftMessages.add(thriftMessage);
  }

  @Override
  public void append(byte[] message) throws TException {
    rawMessages.add(message);
  }

  @Override
  public void close() {
    thriftMessages.clear();
  }

  // Get all the messages appended so far.
  public List<TBase> getMessages() {
    return thriftMessages;
  }

  public List<byte[]> getRawMessages() {
    return rawMessages;
  }

  // Clean up the messages appended so far.
  public void clearMessages() {
    thriftMessages.clear();
    rawMessages.clear();
  }
}
