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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.twitter.ostrich.stats.Stats;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Pattern;

/**
 * Reader that reads a text message from a file.
 *
 * Note that this reader only support UTF-8 encoding.
 */
public class TextMessageReader implements Closeable {

  protected static final HashSet<String> matchEachLineRegexs = new HashSet<>(
      Arrays.asList("^.*$", "^"));

  // The ByteOffsetInputStream to read from.
  private final ByteOffsetInputStream byteOffsetInputStream;

  private final int maxMessageLength;
  private final Pattern messageStartPattern;
  private final boolean matchEachLine;

  // byte offset of the first byte of next line
  private long nextLineStartByteOffset;
  // Next line to be read. It should be null or the first line of the next message.
  // We buffer the first line of next message so that we do not need to reset byteOffset and
  // clear buffer.
  private boolean nextStartLine;
  private ByteBuffer lineBuffer;
  private ByteBuffer messageBuffer;

  public TextMessageReader(
      String path,
      int readBufferSize,
      int maxMessageLength,
      Pattern messageStartPattern) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
    this.maxMessageLength = maxMessageLength;
    this.byteOffsetInputStream = new ByteOffsetInputStream(
        new RandomAccessFile(path, "r"), readBufferSize);
    this.messageStartPattern = messageStartPattern;
    this.nextStartLine = false;
    this.nextLineStartByteOffset = 0L;
    this.matchEachLine = matchEachLineRegexs.contains(messageStartPattern.pattern());
    this.lineBuffer = ByteBuffer.allocate(maxMessageLength);
    this.messageBuffer = ByteBuffer.allocate(maxMessageLength);
  }

  /**
   * Read one text message up to maxMessageLength.
   *
   * @param eofAsMessageBoundary where EOF can be used as message boundary.
   * @return next text message up to the max message size from the reader. null if no text
   * messages in the reader.
   * @throws IOException on file error.
   */
  public ByteBuffer readMessage(boolean eofAsMessageBoundary) throws IOException {
    // If we do not have anything in buffer and in the input stream, we have nothing to be read.
    // Return null in this case.
    if (!nextStartLine && byteOffsetInputStream.isEOF()) {
      return null;
    }
    long messageStartOffset = getByteOffset();

    resetByteBuffer(messageBuffer);
    if (nextStartLine) {
      // If we have the nextStartLine buffered, consume it.
      messageBuffer.put(lineBuffer);
      nextStartLine = false;
    }

    // Add lines to the message until we have the first line of the next message or until we do
    // not have a full line.
    nextStartLine = readLine(maxMessageLength);
    while (nextStartLine && !firstTextMessageLine(lineBuffer)) {
      if (messageBuffer.position() + lineBuffer.limit() <= maxMessageLength) {
        // Take the current line if we do not exceed the maxMessageLength with the
        // current line.
        messageBuffer.put(lineBuffer);
        // note buffer needs to be copied here to save space
      } else {
        // Skip the current line if we already exceed maxMessageLength.
        Stats.incr("singer.reader.textMessageReader.bytesSkipped", lineBuffer.position());
      }
      // Read the next line.
      nextStartLine = readLine(maxMessageLength);
    }

    if (!nextStartLine && !(byteOffsetInputStream.isEOF() && eofAsMessageBoundary)) {
      // If we can not find the next message boundary (start line of next message or EOF and
      // eofAsMessageBoundary is true), we do not have a full message.
      // In this case, we reset the byteOffset to the start of current message and return null.
      setByteOffset(messageStartOffset);
      return null;
    } else {
      if (messageBuffer.position() > 0) {
        // We have read the first line of next message and current message is not empty,
        // return the current message and leave the first line of next message in buffer.
        messageBuffer.flip();
        return messageBuffer;
      } else {
        // We have read the first line of next message and current message is empty,
        // return the next message.
        return readMessage(eofAsMessageBoundary);
      }
    }
  }

  private boolean firstTextMessageLine(ByteBuffer buf) {
    return (buf != null) && (matchEachLine || messageStartPattern.matcher(bufToString(buf)).find());
  }

  public static String bufToString(ByteBuffer buffer) {
    String string = new String(buffer.array(), 0, buffer.limit(), Charsets.UTF_8);
    return string;
  }

  /**
   * Read one text line up to the maxLineLength.
   *
   * @return return true if the next line is read and false if we reached EOF
   */
  public boolean readLine(int maxLineLength) throws IOException {
    if (byteOffsetInputStream.isEOF()) {
      return false;
    }
    nextLineStartByteOffset = byteOffsetInputStream.getByteOffset();

    // Allocate the line buffer for the line.
    int aByte = byteOffsetInputStream.read();
    int index = 0;
    resetByteBuffer(lineBuffer);
    int bytesSkipped = 0;
    while (aByte != -1 && aByte != '\n') {
      if (index >= (maxLineLength - 1)) {
        ++bytesSkipped;
      } else {
        index++;
        lineBuffer.put((byte) aByte);
      }

      aByte = byteOffsetInputStream.read();
    }

    Stats.incr("singer.reader.textMessageReader.bytesSkipped", bytesSkipped);

    if (aByte == -1) {
      // We have reached eof before the end of the line. There is no complete line remaining in
      // this file. So reset byteOffset to line start and return null.
      setByteOffset(nextLineStartByteOffset);
      return false;
    } else {
      // We reach the end of the line. Add the newline char and return the line.
      lineBuffer.put((byte) '\n');
      lineBuffer.flip();
      return true;
    }
  }

  /**
   * @return byte offset of the next message.
   * @throws IOException on file error.
   */
  public long getByteOffset() throws IOException {
    if (nextStartLine) {
      return nextLineStartByteOffset;
    } else {
      return byteOffsetInputStream.getByteOffset();
    }
  }

  public static void resetByteBuffer(ByteBuffer buf) {
    buf.rewind();
    buf.limit(buf.capacity());
  }

  /**
   * Set byte offset of the next message to be read.
   *
   * @param byteOffset byte offset.
   * @throws IOException on file error.
   */
  public void setByteOffset(long byteOffset) throws IOException {
    // If we already at the byte offset, return.
    if (getByteOffset() == byteOffset) {
      return;
    }

    // Clear the buffer.
    nextStartLine = false;
    nextLineStartByteOffset = byteOffset;
    // Set underlying stream byte offset
    byteOffsetInputStream.setByteOffset(byteOffset);
  }

  /**
   * Close the reader.
   *
   * @throws IOException on file error.
   */
  public void close() throws IOException {
    byteOffsetInputStream.close();
    nextStartLine = false;
    nextLineStartByteOffset = 0L;
  }
}
