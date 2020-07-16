package com.pinterest.singer.writer.memq.commons;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32;

import com.pinterest.singer.writer.memq.producer.TaskRequest;

public class MemqMessageHeader {

  private short headerLength;
  private short version;
  private short bytesToSkip;
  private byte[] additionalInfo;
  private int crc;
  private byte compression;
  private int logmessageCount;
  private int batchLength;

  private TaskRequest taskRequest;

  public MemqMessageHeader(ByteBuffer buf) {
    headerLength = buf.getShort();
    version = buf.getShort();
    bytesToSkip = buf.getShort();
    if (bytesToSkip > 0) {
      additionalInfo = new byte[bytesToSkip];
      buf.get(additionalInfo);
    } else {
      additionalInfo = new byte[0];
    }
    crc = buf.getInt();
    compression = buf.get();
    logmessageCount = buf.getInt();
    batchLength = buf.getInt();
  }

  public MemqMessageHeader(DataInputStream stream) throws IOException {
    headerLength = stream.readShort();
    version = stream.readShort();
    bytesToSkip = stream.readShort();
    if (bytesToSkip > 0) {
      additionalInfo = new byte[bytesToSkip];
      stream.readFully(additionalInfo);
    } else {
      additionalInfo = new byte[0];
    }
    crc = stream.readInt();
    compression = stream.readByte();
    logmessageCount = stream.readInt();
    batchLength = stream.readInt();
  }

  /**
   * @param taskRequest
   */
  public MemqMessageHeader(TaskRequest taskRequest) {
    this.taskRequest = taskRequest;
  }

  public short getHeaderLength() {
    return 2 // header length encoding
        + 2 // version of the header
        + 2 // extra header content
        // placeholder for any additional headers info to add
        + 4 // bytes for crc of the message body
        + 1 // compression scheme
        + 4 // for count of logmessages in the body
        + 4 // bytes for length of the message body
    ;
  }

  public void writeHeader(byte[] bAry) {
    CRC32 crc = new CRC32();
    ByteBuffer wrap = ByteBuffer.wrap(bAry);
    wrap.putShort(getHeaderLength()); // 2bytes
    wrap.putShort(taskRequest.getVersion()); // 2bytes
    // add extra stuff
    byte[] extraHeaderContent = getExtraHeaderContent();
    if (extraHeaderContent == null) {
      extraHeaderContent = new byte[0];
    }
    wrap.putShort((short) extraHeaderContent.length);// 2bytes
    if (extraHeaderContent.length > 0) {
      wrap.put(extraHeaderContent);
    }

    ByteBuffer tmp = wrap.duplicate();
    tmp.position(getHeaderLength());
    ByteBuffer slice = tmp.slice();
    crc.update(slice);
    int checkSum = (int) crc.getValue();
    // write crc checksum of the body
    wrap.putInt(checkSum);// 4bytes
    // compression scheme encoding
    wrap.put(this.taskRequest.getCompression().id);// 1byte
    wrap.putInt(this.taskRequest.getLogmessageCount()); // 4bytes
    // write the length of remaining bytes
    int payloadLength = bAry.length - getHeaderLength();
    wrap.putInt(payloadLength); // 4bytes
  }

  private byte[] getExtraHeaderContent() {
    // we don't have anything to add at the moment
    return null;
  }

  public short getVersion() {
    return version;
  }

  public short getBytesToSkip() {
    return bytesToSkip;
  }

  public byte[] getAdditionalInfo() {
    return additionalInfo;
  }

  public int getCrc() {
    return crc;
  }

  public byte getCompression() {
    return compression;
  }

  public int getBatchLength() {
    return batchLength;
  }

  public int getLogmessageCount() {
    return logmessageCount;
  }

  @Override
  public String toString() {
    return "BatchHeader [headerLength=" + headerLength + ", version=" + version + ", bytesToSkip="
        + bytesToSkip + ", additionalInfo=" + Arrays.toString(additionalInfo) + ", crc=" + crc
        + ", compression=" + compression + ", logmessageCount=" + logmessageCount + ", batchLength="
        + batchLength + "]";
  }

}
