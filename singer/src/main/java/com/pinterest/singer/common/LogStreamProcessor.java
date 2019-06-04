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

import com.pinterest.singer.common.errors.LogStreamProcessorException;
import java.io.Closeable;

/**
 * Represent a processor that processes all LogMessages in a LogStream.
 */
public interface LogStreamProcessor extends Closeable {

  /**
   * @return the LogStream processed by this LogStreamProcessor
   */
  LogStream getLogStream();

  /**
   * Process the LogStream.
   *
   * @return Number of LogMessage processed.
   * @throws LogStreamProcessorException when processor can not process the LogStream.
   */
  long processLogStream() throws LogStreamProcessorException;

  /**
   * Start the LogStream processor.
   */
  void start();

  /**
   * Stop the LogStream processor.
   */
  void stop();

  /**
   * Get the latest modification time for the files in the log stream.
   * @return A long value representing the time the file was last modified,
   * measured in milliseconds since the epoch (00:00:00 GMT, January 1, 1970),
   * or 0L if the file does not exist or if an I/O error occurs
   */
  long getLastModificationTimeProcessed();

  /**
   * Helper method to return the last time a cycle was completed by this processor.
   * This information is used to check if we can cleanup kube pod 
   * streams after timeout. 
   * 
   * @return the last time a cycle was completed by this processor
   */
  long getLastCompleteCycleTime();
}
