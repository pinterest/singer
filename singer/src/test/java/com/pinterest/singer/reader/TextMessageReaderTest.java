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
package com.pinterest.singer.reader;

import com.pinterest.singer.SingerTestBase;
import com.pinterest.singer.utils.TextLogger;

import com.google.common.base.Joiner;
import org.apache.commons.io.FilenameUtils;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

/**
 * Test TextMessageReader.
 */
public class TextMessageReaderTest extends SingerTestBase {

  private int MAX_MESSAGE_SIZE = 512;

  private static String[] SINGLE_LINE_MESSAGE_0 = {
      "Single line 0 part 1",
      "Single line 0 part 2\n"
  };

  private static String[] SINGLE_LINE_MESSAGE_1 = {
      "Singe line 1 part 1", "Single line 1 part 2", "Singe line 1 part 3\n"
  };

  private static String[][] MULTI_LINE_MESSAGE_0 = {
      {"I123 multiline 0 line 0 part 1", " multiline 0 line 0 part 2\n"},
      {"    multiline 0 line 1 part 1", " multiline 0 line 1 part 2\n"}
  };

  private static String[][] MULTI_LINE_MESSAGE_1 = {
      {"I124 multiline 1 line 0 part 1", " multiline 1 line 0 part 2",
       " multiline 1 line 0 part 3\n"},
      {"multiline 1 line 1 part 1", " multiline 1 line 1 part 2\n"}
  };

  private static String[][] MULTI_LINE_MESSAGE_2 = {
      {"I124 third message\n"}
  };

  private static String[][] MULTI_LINE_MESSAGE_3 = {
      {"I124 forth message\n"}
  };

  private static String[] VERY_LONG_MULTI_LINE_MESSAGE = {
      "W123 Very looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo\n",
      "Very loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo\n",
      "Very loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo\n",
      "Very loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo\n",
      "Very loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo\n",
      "Very loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo\n",
      "Very loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo\n"
  };

  @Test
  public void testSingleLineTextMessages() throws Exception {
    String path = FilenameUtils.concat(getTempPath(), "text.log");
    TextLogger logger = new TextLogger(path);
    TextMessageReader reader = new TextMessageReader(
        path, 8192, MAX_MESSAGE_SIZE, Pattern.compile("^"));

    // Write first part of the first line.
    logger.logText(SINGLE_LINE_MESSAGE_0[0]);
    // We do not have full line yet. Return null and offset should be 0.
    assertNull(reader.readMessage(false));
    assertEquals(0L, reader.getByteOffset());

    // Write last part of the first line. Now we have a full line.
    logger.logText(SINGLE_LINE_MESSAGE_0[1]);
    // We did not see the first line of the next message. Return null and offset should be 0.
    assertNull(reader.readMessage(false));
    assertEquals(0L, reader.getByteOffset());

    // Get offset of the first byte of second message.
    long byteOffsetOfStartOfSecondMessage = logger.getByteOffset();

    // Write first part of the second line.
    logger.logText(SINGLE_LINE_MESSAGE_1[0]);
    // We have not got a full line of the second message. Return null and offset should be 0.
    assertNull(reader.readMessage(false));
    assertEquals(0L, reader.getByteOffset());

    // Write second part of the second line.
    logger.logText(SINGLE_LINE_MESSAGE_1[1]);
    // We have not got a full line of the second message. Return null and offset should be 0.
    assertNull(reader.readMessage(false));
    assertEquals(0L, reader.getByteOffset());

    // Write last part of the second line. We now seen the first line of the second message.
    // Return the first line and offset of the start of second message.
    logger.logText(SINGLE_LINE_MESSAGE_1[2]);
    assertEquals(Joiner.on("").join(SINGLE_LINE_MESSAGE_0), TextMessageReader.bufToString(reader.readMessage(false)));
    assertEquals(byteOffsetOfStartOfSecondMessage, reader.getByteOffset());

    reader.close();
  }

  @Test
  public void testMultiLineTextMessages() throws Exception {
    String path = FilenameUtils.concat(getTempPath(), "text.log");
    TextLogger logger = new TextLogger(path);
    TextMessageReader reader = new TextMessageReader(
        path, 8192, MAX_MESSAGE_SIZE, Pattern.compile("[IWF][0-9]{3,} "));

    // Write first part of the first line of the first message.
    logger.logText(MULTI_LINE_MESSAGE_0[0][0]);
    // We do not have full line yet. Return null and offset should be 0.
    assertNull(reader.readMessage(false));
    assertEquals(0L, reader.getByteOffset());

    // Write second part of the first line of the first message.
    logger.logText(MULTI_LINE_MESSAGE_0[0][1]);
    // We have a full line, but does not see the first line of the next message. Return null and
    // offset should be 0.
    assertNull(reader.readMessage(false));
    assertEquals(0L, reader.getByteOffset());

    // Write the second line of the first message.
    logger.logText(Joiner.on("").join(MULTI_LINE_MESSAGE_0[1]));
    // We have two full line, but does not see the first line of the next message. Return null and
    // offset should be 0.
    assertNull(reader.readMessage(false));
    assertEquals(0L, reader.getByteOffset());

    long byteOffsetOfStartOfSecondMessage = logger.getByteOffset();

    // Write first part of the first line of the second message.
    logger.logText(MULTI_LINE_MESSAGE_1[0][0]);
    // We did not see the first line of the next message. Return null and offset should be 0.
    assertNull(reader.readMessage(false));
    assertEquals(0L, reader.getByteOffset());

    // Write second part of the first line of the second message.
    logger.logText(MULTI_LINE_MESSAGE_1[0][1]);
    // We did not see the first line of the next message. Return null and offset should be 0.
    assertNull(reader.readMessage(false));
    assertEquals(0L, reader.getByteOffset());

    // Write the last part of the first line of the second message.
    logger.logText(MULTI_LINE_MESSAGE_1[0][2]);
    // We have the first line of the next message. Return the first message and offset should be
    // start offset of second message.
    assertEquals(
        Joiner.on("").join(
            Joiner.on("").join(MULTI_LINE_MESSAGE_0[0]),
            Joiner.on("").join(MULTI_LINE_MESSAGE_0[1])),
        TextMessageReader.bufToString(reader.readMessage(false)));
    assertEquals(byteOffsetOfStartOfSecondMessage, reader.getByteOffset());

    // Write first part of the second line of second message.
    logger.logText(MULTI_LINE_MESSAGE_1[1][0]);
    // We have not got a full second line of second message. Return null and offset should be
    // start byte offset of second message.
    assertNull(reader.readMessage(false));
    assertEquals(byteOffsetOfStartOfSecondMessage, reader.getByteOffset());

    // Write last part of the second line of second message.
    logger.logText(MULTI_LINE_MESSAGE_1[1][1]);
    // We have the full second line of second message. But we do not have the first line of
    // the next message (third message). Return null and offset should be
    // start byte offset of second message.
    assertNull(reader.readMessage(false));
    assertEquals(byteOffsetOfStartOfSecondMessage, reader.getByteOffset());

    long byteOffsetOfStartOfThirdMessage = logger.getByteOffset();
    // Write the third message.
    logger.logText(MULTI_LINE_MESSAGE_2[0][0]);

    // We have the first line of the third message. Return the second message and the start
    // offset of the third message.
    assertEquals(
        Joiner.on("").join(
            Joiner.on("").join(MULTI_LINE_MESSAGE_1[0]),
            Joiner.on("").join(MULTI_LINE_MESSAGE_1[1])
        ),
        TextMessageReader.bufToString(reader.readMessage(false)));
    assertEquals(byteOffsetOfStartOfThirdMessage, reader.getByteOffset());

    long byteOffsetOfStartOfForthMessage = logger.getByteOffset();
    // Write the forth message.
    logger.logText(MULTI_LINE_MESSAGE_3[0][0]);
    // We have the first line of the forth message. Return the third message and the start
    // offset of the forth message.
    assertEquals(MULTI_LINE_MESSAGE_2[0][0], TextMessageReader.bufToString(reader.readMessage(false)));
    assertEquals(byteOffsetOfStartOfForthMessage, reader.getByteOffset());

    // We can not return the forth message since we have not seen the first line of the fifth
    // message. Return the start offset of the forth message
    assertNull(reader.readMessage(false));
    assertEquals(byteOffsetOfStartOfForthMessage, reader.getByteOffset());

    // EOF as message boundary. We should return the forth message and the offset of next message.
    long byteOffsetOfStartOfFifthMessage = logger.getByteOffset();
    assertEquals(MULTI_LINE_MESSAGE_3[0][0], TextMessageReader.bufToString(reader.readMessage(true)));
    assertEquals(byteOffsetOfStartOfFifthMessage, reader.getByteOffset());

    logger.logText(Joiner.on("").join(VERY_LONG_MULTI_LINE_MESSAGE));
    ByteBuffer message = reader.readMessage(true);
    assertTrue(message.position() <= MAX_MESSAGE_SIZE);

    reader.close();
  }

  @Test
  public void testMultipleContinuousReads() throws IOException {
    String path = FilenameUtils.concat(getTempPath(), "test.log");
    TextLogger logger = new TextLogger(path);
    TextMessageReader reader = new TextMessageReader(
        path, 1024, 1024 * 100, Pattern.compile("^.*$"));
    List<String> dataWritten = new ArrayList<>();
    for(int i=0; i<100; i++) {
      StringBuilder builder = new StringBuilder();
      for(int j=0;j<ThreadLocalRandom.current().nextInt(10, 100);j++) {
        builder.append(UUID.randomUUID().toString());
      }
      builder.append('\n');
      String str = builder.toString();
      dataWritten.add(str);
      logger.logText(str);
    }

    for(int i=0; i<100; i++) {
      ByteBuffer msg = reader.readMessage(true);
      assertEquals(dataWritten.get(i), TextMessageReader.bufToString(msg));
    }
    reader.close();
  }
  
  @Test
  public void testLongMessageSkip() throws IOException {
    String path = FilenameUtils.concat(getTempPath(), "test.log");
    TextLogger logger = new TextLogger(path);
    TextMessageReader reader = new TextMessageReader(path, 1024, 1024, Pattern.compile("^.*$"));

    List<String> dataWritten = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      StringBuilder builder = new StringBuilder();
      for (int j = 0; j < ThreadLocalRandom.current().nextInt(200, 2048); j++) {
        builder.append("abcd");
      }
      builder.append('\n');
      String str = builder.toString();
      dataWritten.add(str);
      logger.logText(str);
    }

    for (int i = 0; i < 10; i++) {
      String string = dataWritten.get(i);
      int length = string.length();
      if (length > 1024) {
        length = 1023;
        string = string.substring(0, length) + "\n";
      }
      ByteBuffer msg = reader.readMessage(true);
      assertEquals("Mismatch on index:" + i + " length:" + length, string,
          TextMessageReader.bufToString(msg));
    }
    reader.close();
  }
}