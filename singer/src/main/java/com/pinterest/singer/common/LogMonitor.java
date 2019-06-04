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

import com.pinterest.singer.common.errors.LogMonitorException;

/**
 * Represent a monitor that monitor all configured LogStreams.
 * <p/>
 * The LogMonitor will discover and monitor all configured SingerLogs for LogStreams in those
 * SingerLogs. It will drive the processing of those LogStreams.
 */
public interface LogMonitor {

  /**
   * Monitor LogStreams in all configured logs.
   *
   * @throws LogMonitorException when fail to monitor the SingerLogs configured.
   */
  void monitorLogs() throws LogMonitorException;

  /**
   * Start to periodically monitor LogStreams in all configured logs.
   */
  void start();

  /**
   * Stop monitoring and processing the LogStreams.
   */
  void stop();

}