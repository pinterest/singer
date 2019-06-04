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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

public class MockTypedMessageBuilder implements TypedMessageBuilder<byte[]> {

  private static final long serialVersionUID = 1L;

  @Override
  public TypedMessageBuilder<byte[]> disableReplication() {
    return this;
  }

  @Override
  public TypedMessageBuilder<byte[]> eventTime(long arg0) {
    return this;
  }

  @Override
  public TypedMessageBuilder<byte[]> key(String arg0) {
    return this;
  }

  @Override
  public TypedMessageBuilder<byte[]> keyBytes(byte[] arg0) {
    return this;
  }

  @Override
  public TypedMessageBuilder<byte[]> properties(Map<String, String> arg0) {
    return this;
  }

  @Override
  public TypedMessageBuilder<byte[]> property(String arg0, String arg1) {
    return this;
  }

  @Override
  public TypedMessageBuilder<byte[]> replicationClusters(List<String> arg0) {
    return this;
  }

  @Override
  public MessageId send() throws PulsarClientException {
    return MessageId.latest;
  }

  @Override
  public CompletableFuture<MessageId> sendAsync() {
    CompletableFuture<MessageId> completableFuture = new CompletableFuture<>();
    completableFuture.complete(MessageId.earliest);
    return completableFuture;
  }

  @Override
  public TypedMessageBuilder<byte[]> sequenceId(long arg0) {
    return this;
  }

  @Override
  public TypedMessageBuilder<byte[]> value(byte[] arg0) {
    return this;
  }

}