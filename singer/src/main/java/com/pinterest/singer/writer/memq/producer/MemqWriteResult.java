package com.pinterest.singer.writer.memq.producer;

public class MemqWriteResult {

  private long clientRequestId;
  private int writeLatency;
  private int ackLatency;
  private int bytesWritten;

  public MemqWriteResult() {
  }

  public MemqWriteResult(long clientRequestId, int writeLatency, int ackLatency, int bytesWritten) {
    super();
    this.writeLatency = writeLatency;
    this.ackLatency = ackLatency;
    this.bytesWritten = bytesWritten;
  }

  public int getWriteLatency() {
    return writeLatency;
  }

  public void setWriteLatency(int writeLatency) {
    this.writeLatency = writeLatency;
  }

  public int getAckLatency() {
    return ackLatency;
  }

  public void setAckLatency(int ackLatency) {
    this.ackLatency = ackLatency;
  }

  public int getBytesWritten() {
    return bytesWritten;
  }

  public void setBytesWritten(int bytesWritten) {
    this.bytesWritten = bytesWritten;
  }
  
  public long getClientRequestId() {
    return clientRequestId;
  }

}
