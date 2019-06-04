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
package com.pinterest.singer.utils;

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerLog;

/**
 * Stats utility functions.
 */
public final class StatsUtils {

  private StatsUtils() {
  }

  public static String getLogStreamStatName(LogStream logStream, String group, String stat) {
    return String.format("singer.%s.%s.%s", logStream.getLogStreamDescriptor(), group, stat);
  }

  public static String getLogStatName(SingerLog singerLog, String stat) {
    return String.format("singer.%s.%s", singerLog.getLogName(), stat);
  }
}
