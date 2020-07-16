package com.pinterest.singer.writer.memq.producer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Callable;
import java.util.zip.GZIPOutputStream;

import com.github.luben.zstd.ZstdOutputStream;
import com.pinterest.singer.writer.memq.commons.Compression;
import com.pinterest.singer.writer.memq.commons.MemqMessageHeader;

public abstract class TaskRequest implements Callable<MemqWriteResult> {

  protected Compression compression;
  protected int logmessageCount;

  public abstract OutputStream getOutputStream();

  public abstract int remaining();

  public abstract void markReady() throws IOException;

  public abstract int size();

  public abstract long getId();

  public OutputStream prepareOutputStream(ByteArrayOutputStream stream) throws IOException {
    OutputStream byteBuffer;

    int headerLength = getHeader().getHeaderLength();
    stream.write(new byte[headerLength]);
    if (compression == Compression.GZIP) {
      byteBuffer = new GZIPOutputStream(stream, 8192, true);
    } else if (compression == Compression.ZSTD) {
      byteBuffer = new ZstdOutputStream(stream);
    } else {
      byteBuffer = stream;
    }
    return byteBuffer;
  }

  protected void incrementLogMessageCount() {
    logmessageCount++;
  }

  protected abstract MemqMessageHeader getHeader();

  public Compression getCompression() {
    return compression;
  }

  protected void setCompression(Compression compression) {
    this.compression = compression;
  }

  public abstract short getVersion();

  public int getLogmessageCount() {
    return logmessageCount;
  }

}
