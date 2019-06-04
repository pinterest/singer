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
package com.pinterest.singer.e2e;

import com.google.gson.JsonObject;
import com.pinterest.singer.thrift.Event;
import com.pinterest.singer.client.ThriftLogger;
import com.pinterest.singer.client.ThriftLoggerConfig;
import com.pinterest.singer.client.ThriftLoggerFactory;

import java.io.File;

public class LogWriter implements Runnable {

  private String logName;
  private String logDir;
  private int rotateThresholdKilobytes;
  private int numTestMessages;
  private Thread thread = null;

  public LogWriter(String logName, String dir, int rotateThresholdInKb, int numMessages) {
    this.logName = logName;
    this.logDir = dir;
    this.rotateThresholdKilobytes = rotateThresholdInKb;
    this.numTestMessages = numMessages;
  }

  public void start() {
    thread = new Thread(this);
    thread.start();
  }

  public void run() {
    File outputDir = new File(logDir);
    ThriftLoggerFactory.initialize();
    ThriftLoggerConfig loggerConfig = new ThriftLoggerConfig(
        outputDir, logName, 86400, rotateThresholdKilobytes * 1024);
    ThriftLogger logger = ThriftLoggerFactory.getLogger(loggerConfig);
    try {
      for (int i = 0; i < numTestMessages; i++) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("timestamp", System.currentTimeMillis());
        jsonObject.addProperty("message", "Singer tutorial thrift log message " + i);
        logger.append(jsonObject.toString().getBytes());
        if (i % 100000 == 0) {
          Thread.sleep(300);
        }
      }
      logger.close();
    } catch (Exception e) {
      e.printStackTrace();
      assert (false);
    }
  }
}

