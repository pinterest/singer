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
package com.pinterest.singer.heartbeat;

import com.pinterest.singer.common.HeartbeatWriter;
import com.pinterest.singer.common.SingerStatus;
import com.pinterest.singer.thrift.configuration.SingerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatGenerator implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatGenerator.class);
  private HeartbeatWriter heartbeatWriter;

  public HeartbeatGenerator(SingerConfig singerConfig) {
    heartbeatWriter = new HeartbeatKafkaWriter(singerConfig.heartbeatWriterConfig);
  }

  @Override
  public void run() {
    String oldName = Thread.currentThread().getName();
    try {
      Thread.currentThread().setName("SingerHeartbeat");

      LOG.info("Start sending heartbeat message");
      SingerStatus status = new SingerStatus(System.currentTimeMillis());

      heartbeatWriter.write(status);
      LOG.info("Finished sending heartbeat message");
    } catch (Exception e) {
      LOG.error("Caught exception while sending heartbeat message", e);
    } finally {
      Thread.currentThread().setName(oldName);
    }
  }

  public void stop() {
    try {
      heartbeatWriter.close();
    } catch (Exception e) {
      LOG.error("Failed to close heartbeat writer.", e);
    }
  }
}
