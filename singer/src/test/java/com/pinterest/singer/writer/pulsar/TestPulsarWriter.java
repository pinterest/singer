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

import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.pulsar.client.api.Producer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.pinterest.singer.common.errors.LogStreamWriterException;
import com.pinterest.singer.thrift.LogMessage;

@RunWith(MockitoJUnitRunner.class)
public class TestPulsarWriter {

  private static final int NUMBER_OF_MESSAGES = 2134;
  @Mock
  private Producer<byte[]> mockProducer;

  @Test
  public void testSendAndFlush() throws IOException {
    PulsarWriter writer = new PulsarWriter();
    writer.setProducer(mockProducer);

    when(mockProducer.newMessage()).thenReturn(new MockTypedMessageBuilder());
    List<LogMessage> messages = new ArrayList<>();

    for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
      LogMessage message = new LogMessage();
      message.setKey(String.valueOf(i).getBytes());
      message.setMessage(UUID.randomUUID().toString().getBytes());
      messages.add(message);
    }
    try {
      writer.writeLogMessages(messages);
    } catch (LogStreamWriterException e) {
      e.printStackTrace();
      fail("No errors must be thrown");
    }
    
    verify(mockProducer, times(NUMBER_OF_MESSAGES)).newMessage();
    verify(mockProducer, times(1)).flush();
    
    writer.close();
  }

}