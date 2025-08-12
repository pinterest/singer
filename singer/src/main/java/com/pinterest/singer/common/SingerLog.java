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

import com.pinterest.singer.thrift.configuration.SingerLogConfig;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Represent a log which can have multiple LogStreams in it.
 * All LogStreams in a SingerLog shares the same SingerLogConfig.
 */
public class SingerLog {

  // The config for the SingerLog.
  private final SingerLogConfig singerLogConfig;
  private String podUid;
  private Map<String, ByteBuffer> podMetadata = new HashMap<>();

  public SingerLog(SingerLogConfig singerLogConfig) {
    this.singerLogConfig = Preconditions.checkNotNull(singerLogConfig);
  }

  public SingerLog(SingerLogConfig singerLogConfig, String podUid) {
    this.podUid = podUid;
    this.singerLogConfig = Preconditions.checkNotNull(singerLogConfig);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SingerLog singerLog = (SingerLog) o;
    return singerLogConfig.equals(singerLog.singerLogConfig);
  }

  @Override
  public int hashCode() {
    return singerLogConfig.hashCode();
  }

  /**
   * @return the name of this SingerLog.
   */
  public String getLogName() {
    return singerLogConfig.getName();
  }

  /**
   * @return the config of this SingerLog.
   */
  public SingerLogConfig getSingerLogConfig() {
    return singerLogConfig;
  }
  
  public String getPodUid() {
    return podUid;
  }


  public void addMetadata(String key, ByteBuffer value) {
    podMetadata.put(key, value);
  }

  public Map<String, ByteBuffer> getPodMetadata() {
    return this.podMetadata;
  }

  public void setPodMetadata(Map<String, ByteBuffer> podMetadata) {
    this.podMetadata = podMetadata;
  }

  public Optional<ByteBuffer> getMetadata(String key) {
    return Optional.ofNullable(podMetadata.get(key));
  }
}
