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
  public boolean enableLoggingAudit = false;
  public double auditSamplingRate = 1.0;

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
                            Class<?> thriftClazz,
                            boolean enableLoggingAudit) {
    this.baseDir = baseDir;
    this.kafkaTopic = kafkaTopic;
    this.maxRetentionSecs = maxRetentionSecs;
    this.logRotationThresholdBytes = logRotationThresholdBytes;
    this.thriftClazz = thriftClazz;
    this.enableLoggingAudit = enableLoggingAudit;
  }

  public ThriftLoggerConfig(File baseDir,
                            String kafkaTopic,
                            int maxRetentionSecs,
                            int logRotationThresholdBytes,
                            Class<?> thriftClazz,
                            boolean enableLoggingAudit,
                            double auditSamplingRate) {
    this.baseDir = baseDir;
    this.kafkaTopic = kafkaTopic;
    this.maxRetentionSecs = maxRetentionSecs;
    this.logRotationThresholdBytes = logRotationThresholdBytes;
    this.thriftClazz = thriftClazz;
    this.enableLoggingAudit = enableLoggingAudit;
    this.auditSamplingRate = auditSamplingRate;
  }

  public File getBaseDir() {
    return baseDir;
  }

  public String getKafkaTopic() {
    return kafkaTopic;
  }

  public int getMaxRetentionSecs() {
    return maxRetentionSecs;
  }

  public int getLogRotationThresholdBytes() {
    return logRotationThresholdBytes;
  }

  public Class<?> getThriftClazz() {
    return thriftClazz;
  }

  public void setThriftClazz(Class<?> thriftClazz) {
    this.thriftClazz = thriftClazz;
  }

  public boolean isEnableLoggingAudit() {
    return enableLoggingAudit;
  }

  public void setEnableLoggingAudit(boolean enableLoggingAudit) {
    this.enableLoggingAudit = enableLoggingAudit;
  }

  public double getAuditSamplingRate() {
    return auditSamplingRate;
  }

  public void setAuditSamplingRate(double auditSamplingRate) {
    this.auditSamplingRate = auditSamplingRate;
  }

  public String toString() {
    if (this.thriftClazz != null) {
      return String.format("Thrift Logger config for AuditableLogbackThriftLogger (with advanced "
              + "LoggingAudit feature enabled) is baseDir: %s, kafka topic: %s, max retention("
              + "seconds): %d, log rotation threshold(bytes): %d, thrift class name: %s",
          baseDir.getAbsolutePath(), kafkaTopic, maxRetentionSecs, logRotationThresholdBytes,
          thriftClazz.getName());
    } else if (this.enableLoggingAudit) {
      return String.format("Thrift Logger config for AuditableLogbackThriftLogger (with normal "
              + "LoggingAudit feature enabled) is baseDir: %s, kafka topic: %s, max retention("
              + "seconds): %d, log rotation threshold(bytes): %d",
          baseDir.getAbsolutePath(), kafkaTopic, maxRetentionSecs, logRotationThresholdBytes);

    } else {
      return String.format("Thrift Logger config for LogbackThriftLogger is baseDir: %s,"
              + " kafka topic: %s, max retention(seconds): %d, log rotation threshold(bytes): %d",
          baseDir.getAbsolutePath(), kafkaTopic, maxRetentionSecs, logRotationThresholdBytes);

    }
  }
}