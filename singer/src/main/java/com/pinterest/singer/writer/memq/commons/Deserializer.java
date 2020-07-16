package com.pinterest.singer.writer.memq.commons;

import java.util.Properties;

public interface Deserializer<T> {

  public default void init(Properties props) {
  }

  public T deserialize(byte[] bytes);

}
