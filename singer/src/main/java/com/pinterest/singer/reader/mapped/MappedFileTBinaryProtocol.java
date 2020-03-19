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
package com.pinterest.singer.reader.mapped;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TTransport;

/**
 * SingerTBinaryProtocol is an implementation of FramedTBinaryProtocol reading data
 * from Memory Mapped File. The entire length of file is memory mapped using
 * {@link FileChannel}. This only creates a memory mapping however data will
 * only be read when actual bytes are accessed so this is safe from a memory
 * utilization. It's the responsibility of the underlying operating system to
 * swap pages if sufficient memory is not available.
 *
 * Note that {@link TTransport} can't be used in this case since the
 * {@link TProtocol} needs direct reference to the ByteBuffer for lower level
 * byte manipulations which won't be possible without direct reference.
 */
public class MappedFileTBinaryProtocol extends TProtocol {

  private static final Logger logger = Logger
      .getLogger(MappedFileTBinaryProtocol.class.getCanonicalName());
  private static final TStruct ANONYMOUS_STRUCT = new TStruct();
  private static final long NO_LENGTH_LIMIT = -1;

  protected static final int VERSION_MASK = 0xffff0000;
  protected static final int VERSION_1 = 0x80010000;

  /**
   * The maximum number of bytes to read from the transport for variable-length
   * fields (such as strings or binary) or {@link #NO_LENGTH_LIMIT} for unlimited.
   */
  private final long stringLengthLimit;

  /**
   * The maximum number of elements to read from the network for containers (maps,
   * sets, lists), or {@link #NO_LENGTH_LIMIT} for unlimited.
   */
  private final long containerLengthLimit;

  protected boolean strictRead;
  protected boolean strictWrite;

  private ByteBuffer buffer;
  private File file;

  public MappedFileTBinaryProtocol(File file) throws FileNotFoundException, IOException {
    this(file, false, true);
  }

  public MappedFileTBinaryProtocol(File file,
                                   boolean strictRead,
                                   boolean strictWrite) throws FileNotFoundException, IOException {
    this(file, NO_LENGTH_LIMIT, NO_LENGTH_LIMIT, strictRead, strictWrite);
  }

  public MappedFileTBinaryProtocol(File file,
                                   long stringLengthLimit,
                                   long containerLengthLimit) throws FileNotFoundException,
                                                              IOException {
    this(file, stringLengthLimit, containerLengthLimit, false, true);
  }

  public MappedFileTBinaryProtocol(File file,
                                   long stringLengthLimit,
                                   long containerLengthLimit,
                                   boolean strictRead,
                                   boolean strictWrite) throws FileNotFoundException, IOException {
    super(null);
    this.file = file;
    buffer = mapFile(file);
    this.stringLengthLimit = stringLengthLimit;
    this.containerLengthLimit = containerLengthLimit;
    this.strictRead = strictRead;
    this.strictWrite = strictWrite;
  }

  /**
   * Used to map (memorymapped file) and remap files. Note that file handle is
   * closed immediately after mapping to not run out of file handles. Once we have
   * the memorymap the file handle is no longer needed.
   *
   * @param file
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  protected ByteBuffer mapFile(File file) throws FileNotFoundException, IOException {
    RandomAccessFile raf = new RandomAccessFile(file, "r");
    MappedByteBuffer map = raf.getChannel().map(MapMode.READ_ONLY, 0, file.length());
    raf.close();
    return map;
  }

  /**
   * Reading methods.
   */

  public TMessage readMessageBegin() throws TException {
    int size = readI32();
    if (size < 0) {
      int version = size & VERSION_MASK;
      if (version != VERSION_1) {
        throw new TProtocolException(TProtocolException.BAD_VERSION,
            "Bad version in readMessageBegin");
      }
      return new TMessage(readString(), (byte) (size & 0x000000ff), readI32());
    } else {
      if (strictRead) {
        throw new TProtocolException(TProtocolException.BAD_VERSION,
            "Missing version in readMessageBegin");
      }
      return new TMessage(readStringBody(size), readByte(), readI32());
    }
  }

  public void readMessageEnd() {
  }

  public TStruct readStructBegin() {
    try {
      // since data is framed, each message read is preceded by a 4 byte size
      readI32();
    } catch (TException e) {
      logger.log(Level.SEVERE, "Failed to read frame", e);
    }
    return ANONYMOUS_STRUCT;
  }

  public void readStructEnd() {
  }

  public TField readFieldBegin() throws TException {
    byte type = readByte();
    short id = type == TType.STOP ? 0 : readI16();
    return new TField("", type, id);
  }

  public void readFieldEnd() {
  }

  public TMap readMapBegin() throws TException {
    TMap map = new TMap(readByte(), readByte(), readI32());
    checkContainerReadLength(map.size);
    return map;
  }

  public void readMapEnd() {
  }

  public TList readListBegin() throws TException {
    TList list = new TList(readByte(), readI32());
    checkContainerReadLength(list.size);
    return list;
  }

  public void readListEnd() {
  }

  public TSet readSetBegin() throws TException {
    TSet set = new TSet(readByte(), readI32());
    checkContainerReadLength(set.size);
    return set;
  }

  public void readSetEnd() {
  }

  public boolean readBool() throws TException {
    return (readByte() == 1);
  }

  public byte readByte() throws TException {
    checkAvailable(1);
    return buffer.get();
  }

  /**
   * Checks if the specified number of bytes are available to be read from the
   * byte buffer. If this is not true the method checks if the file has more data
   * to be read and will trigger a remap of the file.
   *
   * @param size
   * @throws TException
   */
  protected void checkAvailable(int size) throws TException {
    if (buffer.remaining() < size) {
      if (file.length() > buffer.capacity()) {
        remap();
      } else {
        throw new TException(
            "No more data available, file.length:" + file.length() + " vs. buffer.capacity:" + buffer.capacity());
      }
    }
  }

  private void remap() throws TException {
    try {
      int position = buffer.position();
      buffer = mapFile(file);
      buffer.position(position);
    } catch (IOException e) {
      throw new TException(e);
    }
  }

  public short readI16() throws TException {
    checkAvailable(Short.BYTES);
    return buffer.getShort();
  }

  public int readI32() throws TException {
    checkAvailable(Integer.BYTES);
    return buffer.getInt();
  }

  public long readI64() throws TException {
    checkAvailable(Long.BYTES);
    return buffer.getLong();
  }

  public double readDouble() throws TException {
    return Double.longBitsToDouble(readI64());
  }

  public String readString() throws TException {
    int size = readI32();
    checkStringReadLength(size);
    return readStringBody(size);
  }

  public String readStringBody(int size) throws TException {
    checkStringReadLength(size);
    checkAvailable(size);
    try {
      byte[] buf = new byte[size];
      buffer.get(buf, 0, size);
      return new String(buf, "UTF-8");
    } catch (UnsupportedEncodingException uex) {
      throw new TException("missing utf-8 support");
    }
  }

  public ByteBuffer readBinary() throws TException {
    int size = readI32();
    checkStringReadLength(size);
    checkAvailable(size);
    int currentPosition = buffer.position();
    ByteBuffer buf = buffer.slice();
    buf.limit(size);
    buffer.position(currentPosition + size);
    return buf;
  }

  public void setByteOffset(int byteOffset) {
    buffer.position(byteOffset);
  }

  public int getBytesRemainingInBuffer() {
    return buffer.remaining();
  }

  public boolean isEOF() {
    return !buffer.hasRemaining();
  }

  public long getByteOffset() {
    return buffer.position();
  }

  private void checkStringReadLength(int length) throws TProtocolException {
    if (length < 0) {
      throw new TProtocolException(TProtocolException.NEGATIVE_SIZE, "Negative length: " + length);
    }
    if (stringLengthLimit != NO_LENGTH_LIMIT && length > stringLengthLimit) {
      throw new TProtocolException(TProtocolException.SIZE_LIMIT,
          "Length exceeded max allowed: " + length);
    }
  }

  private void checkContainerReadLength(int length) throws TProtocolException {
    if (length < 0) {
      throw new TProtocolException(TProtocolException.NEGATIVE_SIZE, "Negative length: " + length);
    }
    if (containerLengthLimit != NO_LENGTH_LIMIT && length > containerLengthLimit) {
      throw new TProtocolException(TProtocolException.SIZE_LIMIT,
          "Length exceeded max allowed: " + length);
    }
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public void writeMessageBegin(TMessage message) throws TException {
    throw new TException("method not supported");
  }

  public void writeMessageEnd() {
  }

  public void writeStructBegin(TStruct struct) {
  }

  public void writeStructEnd() {
  }

  public void writeFieldBegin(TField field) throws TException {
    throw new TException("method not supported");
  }

  public void writeFieldEnd() {
  }

  public void writeFieldStop() throws TException {
    throw new TException("method not supported");
  }

  public void writeMapBegin(TMap map) throws TException {
    throw new TException("method not supported");
  }

  public void writeMapEnd() {
  }

  public void writeListBegin(TList list) throws TException {
    throw new TException("method not supported");
  }

  public void writeListEnd() {
  }

  public void writeSetBegin(TSet set) throws TException {
    throw new TException("method not supported");
  }

  public void writeSetEnd() {
  }

  public void writeBool(boolean b) throws TException {
    throw new TException("method not supported");
  }

  public void writeByte(byte b) throws TException {
    throw new TException("method not supported");
  }

  public void writeI16(short i16) throws TException {
    throw new TException("method not supported");
  }

  public void writeI32(int i32) throws TException {
    throw new TException("method not supported");
  }

  public void writeI64(long i64) throws TException {
    throw new TException("method not supported");
  }

  public void writeDouble(double dub) throws TException {
    throw new TException("method not supported");
  }

  public void writeString(String str) throws TException {
    throw new TException("method not supported");
  }

  public void writeBinary(ByteBuffer bin) throws TException {
    throw new TException("method not supported");
  }

}
