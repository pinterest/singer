package com.pinterest.singer.writer.memq.producer;

public interface Serializer<T> {

  public byte[] serialize(T data);

}
