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

import com.google.common.base.Preconditions;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;

/**
 * Base class for ThriftLoggers, implements the utility helper
 * append() methods which log using the current wall-clock
 * time and no partition key.
 */
public abstract class BaseThriftLogger implements ThriftLogger {

  /**
   * Return the current wall-clock time in nanoseconds.
   */
  private static long getNowTimestampInNanos() {
    return System.currentTimeMillis() * 1000 * 1000;
  }

  public void append(TBase thriftMessage) throws TException {
    Preconditions.checkArgument(
        validateThriftMessageForSecor(thriftMessage));
    append(null, thriftMessage, getNowTimestampInNanos());
  }

  public void append(byte[] message) {
    append(null, message, getNowTimestampInNanos());
  }

  /**
   * Messages to be consumed by Secor must have an i64 timestamp
   * as the first field.
   */
  private boolean validateThriftMessageForSecor(TBase thriftMessage) {
    TFieldIdEnum fieldEnum = thriftMessage.fieldForId(1);
    if (fieldEnum == null) {
      return false;
    }
    if (!thriftMessage.isSet(fieldEnum)) {
      return false;
    }
    Object value = thriftMessage.getFieldValue(fieldEnum);
    if (value == null || value.getClass() != Long.class) {
      return false;
    }
    return true;
  }
}
