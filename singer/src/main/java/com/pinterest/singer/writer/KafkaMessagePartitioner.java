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

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.PartitionInfo;

/**
 * Interface to partition the data using supplied key.
 * 
 * - Partitioner can be stateful<br/>
 * - Supplied partition list doesn't honor any sorting
 * - Returned partition id must be absolute
 */
public interface KafkaMessagePartitioner {
  default void configure(Map<String, String> partitionerConfig) {

  }
  int partition(Object messageKey, List<PartitionInfo> partitions);

}