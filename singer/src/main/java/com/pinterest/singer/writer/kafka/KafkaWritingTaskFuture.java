/**
 * Copyright 2020 Pinterest, Inc.
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
package com.pinterest.singer.writer.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Write Task Future created for tracking partition batch write status when
 * using {@link CommittableKafkaWriter}
 */
public class KafkaWritingTaskFuture {

  private long firstProduceTimestamp;
  public boolean success;
  public Exception exception;
  private int writtenBytesSize;
  private int kafkaBatchWriteLatencyInMillis;
  /**
   * a list of the RecordMetadata for every producer record in a KafkaWritingTask.
   * Initialization is needed to prevent NullPointerException when
   * KafkaWritingTask fails.
   */
  private List<Future<RecordMetadata>> recordMetadataList = new ArrayList<>();
  private PartitionInfo partitionInfo;

  public KafkaWritingTaskFuture(PartitionInfo partitionInfo) {
    this.partitionInfo = partitionInfo;
  }

  public KafkaWritingTaskFuture(boolean successFlag, int bytes, int kafkaLatency) {
    this.success = successFlag;
    this.writtenBytesSize = bytes;
    this.kafkaBatchWriteLatencyInMillis = kafkaLatency;
    this.exception = null;
  }

  public KafkaWritingTaskFuture(boolean successFlag, Exception e) {
    this.success = successFlag;
    this.exception = e;
  }

  public int getWrittenBytesSize() {
    return writtenBytesSize;
  }

  public int getKafkaBatchWriteLatencyInMillis() {
    return kafkaBatchWriteLatencyInMillis;
  }

  public List<Future<RecordMetadata>> getRecordMetadataList() {
    return recordMetadataList;
  }

  public void setRecordMetadataList(List<Future<RecordMetadata>> recordMetadataList) {
    this.recordMetadataList = recordMetadataList;
  }

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public Exception getException() {
    return exception;
  }

  public void setException(Exception exception) {
    this.exception = exception;
  }

  public void setWrittenBytesSize(int writtenBytesSize) {
    this.writtenBytesSize = writtenBytesSize;
  }

  public void setKafkaBatchWriteLatencyInMillis(int kafkaBatchWriteLatencyInMillis) {
    this.kafkaBatchWriteLatencyInMillis = kafkaBatchWriteLatencyInMillis;
  }

  public long getFirstProduceTimestamp() {
    return firstProduceTimestamp;
  }

  public void setFirstProduceTimestamp(long firstProduceTimestamp) {
    this.firstProduceTimestamp = firstProduceTimestamp;
  }

  public PartitionInfo getPartitionInfo() {
    return partitionInfo;
  }

  public void setPartitionInfo(PartitionInfo partitionInfo) {
    this.partitionInfo = partitionInfo;
  }
}
