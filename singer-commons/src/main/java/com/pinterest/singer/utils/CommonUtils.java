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

import com.pinterest.singer.loggingaudit.client.common.LoggingAuditClientMetrics;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;

public class CommonUtils {

  public static int getPid() {
    int pid = -1;
    try {
      String pidStr = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
      pid = Integer.parseInt(pidStr);
      return pid;
    } catch (Exception e){
      return pid;
    }
  }

  public static String getHostName() {
    String hostName = "";
    try {
      hostName = InetAddress.getLocalHost().getHostName();
      int firstDotPos = hostName.indexOf('.');
      if (firstDotPos > 0) {
        hostName = hostName.substring(0, firstDotPos);
      }
    } catch (Exception e) {
      // fall back to env var.
      hostName = System.getenv("HOSTNAME");
    }
    return hostName;
  }

  public static void reportQueueUsage(int qSize, int remainingCapacity, String host, String stage){
    double qUsagePercent = qSize * 1.0 / (qSize + remainingCapacity);
    OpenTsdbMetricConverter.gauge(LoggingAuditClientMetrics.AUDIT_CLIENT_QUEUE_SIZE, qSize,
        "host=" + host, "stage=" + stage);
    OpenTsdbMetricConverter.gauge(LoggingAuditClientMetrics.AUDIT_CLIENT_QUEUE_USAGE_PERCENT,
        qUsagePercent, "host=" + host, "stage=" + stage);
  }

}
