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

import com.pinterest.singer.thrift.LogMessage;
import org.apache.kafka.common.header.Headers;

/**
 * Interface to add headers to ProducerRecord's Headers object. Concrete implementations of this
 * interface could chain their addHeaders(Headers headers, LogMessage logMessage) method to modify
 * the original ProducerRecord's Headers object.
 */
public interface HeadersInjector {

  /**
   * Given ProducerRecord's Headers object and LogMessage object
   *
   * @return The original ProducerRecord's Headers object with some headers added.
   */

   Headers addHeaders(Headers headers, LogMessage logMessage);

}
