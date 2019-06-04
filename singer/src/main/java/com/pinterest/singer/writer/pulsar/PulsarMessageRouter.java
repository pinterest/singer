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
package com.pinterest.singer.writer.pulsar;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TopicMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarMessageRouter implements MessageRouter {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(PulsarMessageRouter.class);
  private PulsarMessagePartitioner partitioner;

  public PulsarMessageRouter(Class<PulsarMessagePartitioner> singerPartitionerClass)
      throws InstantiationException, IllegalAccessException {
    partitioner = singerPartitionerClass.newInstance();
    LOG.debug("Initialized Pulsar with router:" + singerPartitionerClass.getName());
  }

  @Override
  public int choosePartition(Message<?> msg) {
    return choosePartition(msg, null);
  }

  @Override
  public int choosePartition(Message<?> msg, TopicMetadata metadata) {
    int partition = 0;
    if (metadata != null) {
      partition = partitioner.partition(msg.getKeyBytes(), metadata.numPartitions());
    } else {
      partition = partitioner.partition(msg.getKeyBytes(), 1);
    }
    return partition;
  }

}