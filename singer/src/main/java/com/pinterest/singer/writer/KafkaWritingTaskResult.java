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
package com.pinterest.singer.writer;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;

public class KafkaWritingTaskResult {

  public final boolean success;
  public final Exception exception;
  private int writtenBytesSize;
  private int kafkaBatchWriteLatencyInMillis;
  /**
   *  a list of the RecordMetadata for every producer record in a KafkaWritingTask. Initialization
   *  is needed to prevent NullPointerException when KafkaWritingTask fails.
   */
  private List<RecordMetadata> recordMetadataList = new ArrayList<>();
  private int partition = -1;

  public KafkaWritingTaskResult(boolean successFlag, int bytes, int kafkaLatency) {
    this.success = successFlag;
    this.writtenBytesSize = bytes;
    this.kafkaBatchWriteLatencyInMillis = kafkaLatency;
    this.exception = null;
  }

  public KafkaWritingTaskResult(boolean successFlag, Exception e) {
    this.success = successFlag;
    this.exception = e;
  }
  
  public int getWrittenBytesSize() {
    return writtenBytesSize;
  }
  
  public int getKafkaBatchWriteLatencyInMillis() {
    return kafkaBatchWriteLatencyInMillis;
  }

  public void setRecordMetadataList(List<RecordMetadata> recordMetadataList) {
    this.recordMetadataList = recordMetadataList;
  }

  public List<RecordMetadata> getRecordMetadataList() {
    return recordMetadataList;
  }

  public int getPartition() {
    return partition;
  }

  public void setPartition(int partition) {
    this.partition = partition;
  }
}
