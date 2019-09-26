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

import java.io.File;

/**
 * ThriftLoggerConfig is the config class that can be used to configure the thrift logger.
 */
public class ThriftLoggerConfig {

  public final File baseDir;
  public final String kafkaTopic;
  public final int maxRetentionSecs;
  public final int logRotationThresholdBytes;
  public Class<?> thriftClazz;

  public ThriftLoggerConfig(File baseDir,
                            String kafkaTopic,
                            int maxRetentionSecs,
                            int logRotationThresholdBytes) {

    this.baseDir = baseDir;
    this.kafkaTopic = kafkaTopic;
    this.maxRetentionSecs = maxRetentionSecs;
    this.logRotationThresholdBytes = logRotationThresholdBytes;
  }

  public ThriftLoggerConfig(File baseDir,
                            String kafkaTopic,
                            int maxRetentionSecs,
                            int logRotationThresholdBytes,
                            Class<?> thriftClazz) {
    this.baseDir = baseDir;
    this.kafkaTopic = kafkaTopic;
    this.maxRetentionSecs = maxRetentionSecs;
    this.logRotationThresholdBytes = logRotationThresholdBytes;
    this.thriftClazz = thriftClazz;
  }


  public String toString() {
    if (thriftClazz == null) {
      return String.format("Thrift Logger config is baseDir: %s,"
              + " kafka topic: %s, max retention(seconds): %d, log rotation threshold(bytes): %d",
          baseDir.getAbsolutePath(), kafkaTopic, maxRetentionSecs, logRotationThresholdBytes);
    } else {
      return String.format("Thrift Logger config for AuditableLogbackThriftLogger is baseDir: %s,"
              + " kafka topic: %s, max retention(seconds): %d, log rotation threshold(bytes): %d, thrift class name: %s",
          baseDir.getAbsolutePath(), kafkaTopic, maxRetentionSecs, logRotationThresholdBytes, thriftClazz.getName());
    }
  }
}