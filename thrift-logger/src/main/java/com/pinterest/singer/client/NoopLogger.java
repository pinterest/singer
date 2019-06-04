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
 * Noop ThriftLogger which doesn't do anything on append, useful for testing or mocking.
 */
public class NoopLogger implements ThriftLogger {

  @Override
  public void append(byte[] partitionKey, byte[] message, long timeNanos) { }

  @Override
  public void append(byte[] partitionKey, TBase thriftMessage, long timeNanos) throws TException {
  }

  @Override
  public void append(TBase thriftMessage) throws TException {}

  @Override
  public void append(byte[] message) throws TException { }

  @Override
  public void close() {}
}
