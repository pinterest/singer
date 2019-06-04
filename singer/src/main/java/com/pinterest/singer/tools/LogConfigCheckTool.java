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

import java.io.File;
import java.util.Arrays;

import com.pinterest.singer.config.DirectorySingerConfigurator;
import com.pinterest.singer.utils.LogConfigUtils;

/**
 * Tool for checking LogConfig file to be valid
 */
public class LogConfigCheckTool {

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Must specify the configuration directory");
      System.exit(-1);
    }

    String configDirectory = args[0];
    File confDir = new File(configDirectory);

    if (!confDir.isDirectory()) {
      System.out.println("Supplied path is not a directory:" + configDirectory);
      System.exit(-1);
    }

    for (String logFile : confDir.list()) {
      if (!logFile.matches(DirectorySingerConfigurator.CONFIG_FILE_PATTERN)) {
        System.err.println("Invalid configuration file name:" + logFile);
        System.exit(-1);
      }
    }

    File[] listFiles = confDir.listFiles();
    Arrays.sort(listFiles);
    for (File file : listFiles) {
      try {
        LogConfigUtils.parseLogConfigFromFile(file);
      } catch (Exception e) {
        e.printStackTrace();
        System.err.println("Bad configuration file:" + file.getName());
        System.exit(-1);
      }
    }
  }

}