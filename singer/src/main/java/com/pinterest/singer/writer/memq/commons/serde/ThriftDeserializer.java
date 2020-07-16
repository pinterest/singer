package com.pinterest.singer.writer.memq.commons.serde;

import java.util.Properties;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import com.pinterest.singer.writer.memq.commons.Deserializer;

@SuppressWarnings("rawtypes")
public class ThriftDeserializer implements Deserializer<TBase> {

  public static final String TBASE_OBJECT_CONFIG = "tBaseObject";
  private TBase tbase;

  @Override
  public void init(Properties props) {
    if (!props.containsKey(TBASE_OBJECT_CONFIG)) {
      throw new RuntimeException("ThriftDeserializer must have TBASE_OBJECT_CONFIG");
    }
    tbase = (TBase) props.get(TBASE_OBJECT_CONFIG);
  }

  @Override
  public TBase deserialize(byte[] bytes) {
    try {
      TDeserializer tDeserializer = new TDeserializer();
      tDeserializer.deserialize(tbase, bytes);
      return tbase;
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

}
