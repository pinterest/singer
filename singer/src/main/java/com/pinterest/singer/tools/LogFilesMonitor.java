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
package com.pinterest.singer.tools;


import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.monitor.DefaultLogMonitor;
import com.pinterest.singer.monitor.FileSystemMonitor;
import com.pinterest.singer.monitor.LogStreamManager;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.utils.SingerUtils;

import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.logging.LogManager;

/**
 * The tool for checking the file system events from log directories
 *
 *  Usage: LogFileMonitor  singer_config_dir
 */
public class LogFilesMonitor {

  private static void printUsage() {
    System.out.println("Usage: LogFileMonitor singer_config_dir");
  }

  private static void printLogStreamsInfo() throws Exception {
    Collection<LogStream> logStreams = LogStreamManager.getLogStreams();
    System.out.println("LogStreams Info: ");
    for (LogStream stream: logStreams) {
      List<String> files = stream.getLogFilePaths();
      System.out.println(stream + " : ");
      for (String s : files) {
        System.out.println(s + ",");
      }
      System.out.println("\n");
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage();
      return;
    }

    try {
      String configDir = args[0];
      SingerConfig singerConfig = SingerUtils.loadSingerConfig(configDir, null, false);
      List<SingerLogConfig> logConfigs = singerConfig.getLogConfigs();

      SingerSettings.initialize(singerConfig);
      LogStreamManager.getInstance().initializeLogStreams();

      System.out.println(SingerSettings.getOrCreateFileSystemMonitor("").toString());
      printLogStreamsInfo();

      SingerSettings.getOrCreateFileSystemMonitor("").updateAllDirectories();
      System.out.println(SingerSettings.getOrCreateFileSystemMonitor("").toString());
      printLogStreamsInfo();

      SingerSettings.getOrCreateFileSystemMonitor("");
      SingerSettings.getOrCreateFileSystemMonitor("").join();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
