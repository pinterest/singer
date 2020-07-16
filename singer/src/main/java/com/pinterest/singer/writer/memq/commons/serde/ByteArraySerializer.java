package com.pinterest.singer.writer.memq.commons.serde;

import com.pinterest.singer.writer.memq.producer.Serializer;

public class ByteArraySerializer implements Serializer<byte[]> {

  @Override
  public byte[] serialize(byte[] data) {
    return data;
  }

}
