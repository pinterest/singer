package com.pinterest.singer.writer.memq.commons;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;

public enum Compression {
                         GZIP(1, GZIPInputStream.class, GZIPOutputStream.class),
                         NONE(0, null, null),
                         ZSTD(2, ZstdInputStream.class, ZstdOutputStream.class);

  public byte id;
  public Class<? extends InputStream> inputStream;
  public Class<? extends OutputStream> outputStream;

  private Compression(int id,
                      Class<? extends InputStream> inputStream,
                      Class<? extends OutputStream> outputStream) {
    this.inputStream = inputStream;
    this.outputStream = outputStream;
    this.id = (byte) id;
  }
}