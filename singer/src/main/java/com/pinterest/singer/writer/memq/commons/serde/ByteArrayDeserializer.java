package com.pinterest.singer.writer.memq.commons.serde;

import com.pinterest.singer.writer.memq.commons.Deserializer;

public class ByteArrayDeserializer implements Deserializer<byte[]> {

  @Override
  public byte[] deserialize(byte[] bytes) {
    return bytes;
  }

}
