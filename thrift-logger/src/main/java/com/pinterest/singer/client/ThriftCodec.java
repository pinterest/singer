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
package com.pinterest.singer.client;

import com.google.common.base.Charsets;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * A singleton class to serialize and deserialize a thrift object.
 * This class is thread safe.
 *
 * Thrift supports both the TBinaryProtocol and the TCompactProtocol.
 * From benchmarking, TCompactProtocol is between 20% to 30% more
 * efficient in space usage for large thrift objects. We originally
 * encoded most of our data in TBinaryProtocol. To support both,
 * we now can deserialize data encoded in TBinaryProtocol or in a special
 * "prefixed" TCompactProtocol with the deserialize() call. We
 * can choose to serialize data using TBinaryProtocol (the default), or
 * using the "prefixed" TCompactProtocol (serializePrefixed call).
 *
 * TBinaryProtocol's first byte is always field type (see TType.java in
 * thrift), which ranges from 0-16. So by prefixing the TCompactProtocol
 * output with SECRET_BYTE (0xff) we can distinguish between TBinaryProtocol
 * and TCompactProtocol and know which one to use.
 *
 * Usage:
 *  // Serialize using TBinaryProtocol.
 *  ThriftCodec.getInstance().serialize(myThriftObject)
 *  // Serialize using prefixed TCompactProtocol.
 *  ThriftCodec.getInstance().serializePrefixed(myThriftObject)
 *  // Deserialize any of the above two protocols:
 *  ThriftObject myThriftObject = ThriftCodec.deserialize(myBytes, ThriftObject.class);
 *
 */
public class ThriftCodec {
  // CHECKSTYLE_OFF: DeclarationOrder

  private static volatile ThriftCodec instance;

  private ThriftCodec() {
  }

  /**
   * get an instance of the singleton object.
   */
  public static ThriftCodec getInstance() {
    if (instance == null) {
      synchronized (ThriftCodec.class) {
        if (instance == null) {
          instance = new ThriftCodec();
        }
      }
    }
    return instance;
  }

  /**
   * Deserialize a thrift value encoded using TCompactProtocol.
   */
  public static <T extends TBase> T deserializeCompact(byte[] value, Class<T> thriftClass)
      throws TException {
    ThriftCodec codec = getInstance();
    try {
      T obj = thriftClass.newInstance();
      codec.deserializeCompact(obj, value);
      return obj;
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Serialize a thrift object.
   *
   * @param obj A thrift object.
   */
  public <T extends TBase> byte[] serialize(T obj) throws TException {
    byte[] bytes = encoder.get().serialize(obj);
    return bytes;
  }

  /**
   * Serializes thrift objects with the TCompactProtocol
   * prefixed with SECRET_BYTE so that we can differentiate
   * between these and TBinaryProtocol datasets.
   *
   * @param obj a thrift object.
   */
  public <T extends TBase> byte[] serializePrefixed(T obj) throws TException {
    byte[] bytes = prefixedEncoder.get().serialize(obj);
    return bytes;
  }

  /**
   * Serialize a thrift object as JSON.
   *
   * @param obj A thrift object.
   */
  public static <T extends TBase> String serializeJson(T obj) throws TException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    obj.write(new TJSONProtocol(new TIOStreamTransport(byteArrayOutputStream)));
    try {
      return byteArrayOutputStream.toString("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new TException(e);
    }
  }

  /**
   * Deserialize a thrift object encoded using the TJSONProtocol
   */
  public static <T extends TBase> T deserializeJson(String jsonValue, Class<T> thriftClass)
      throws TException {
    ByteArrayInputStream byteArrayInputStream =
        new ByteArrayInputStream(jsonValue.getBytes(Charsets.UTF_8));
    try {
      T obj = thriftClass.newInstance();
      obj.read(new TJSONProtocol(new TIOStreamTransport(byteArrayInputStream)));
      return obj;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deserialize a thrift object encoded using the TCompactProtocol
   */
  public <T extends TBase> void deserializeCompact(T obj, byte[] value) throws TException {
    decoderCompact.get().deserialize(obj, value);
  }

  /**
   * Serialize a thrift object with the TCompactProtocol
   */
  public <T extends TBase> byte[] serializeCompact(T obj) throws TException {
    return encoderCompact.get().serialize(obj);
  }


  /** Check if serialized value was encoded using prefixed compact protocol. */
  public static boolean isPrefixedCompact(byte[] value) {
    return value != null && value.length > 2 && value[0] == PrefixedSerializer.SECRET_BYTE
        && value[1] == PrefixedSerializer.COMPACT_PROTOCOL_BYTE;
  }

  private ThreadLocal<TDeserializer> decoder = new ThreadLocal<TDeserializer>() {
    @Override
    protected TDeserializer initialValue() {
      return new TDeserializer(new TBinaryProtocol.Factory());
    }
  };

  private ThreadLocal<TDeserializer> decoderCompact = new ThreadLocal<TDeserializer>() {
    @Override
    protected TDeserializer initialValue() {
      return new TDeserializer(new TCompactProtocol.Factory());
    }
  };

  /**
   * Serializer to use with PrefixedDeserializer below.
   */
  private static class PrefixedSerializer {

    // Use a secret byte to prefix messages encoded using TCompactProtocol.
    // Messages encoded using TBinaryProtocol will never start with this byte
    // so that's how we can distinguish between TBinaryProtocol and TCompactProtocol.
    public static final byte SECRET_BYTE = (byte) 0xff;
    public static final byte COMPACT_PROTOCOL_BYTE = (byte) 0x01;

    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    /**
     * This transport wraps that byte array
     */
    private final TIOStreamTransport transport = new TIOStreamTransport(outputStream);

    /**
     * Internal protocol used for serializing objects.
     */
    private TProtocol protocol = new TCompactProtocol(transport);

    /**
     * Serialize the Thrift object into a byte array. The process is simple,
     * just clear the byte array output, write the object into it, and grab the
     * raw bytes.
     *
     * @param base The object to serialize
     * @return Serialized object in byte[] format
     */
    public byte[] serialize(TBase base) throws TException {
      outputStream.reset();
      outputStream.write(SECRET_BYTE);
      outputStream.write(COMPACT_PROTOCOL_BYTE);
      base.write(protocol);
      return outputStream.toByteArray();
    }
  }

  private static class PrefixedDeserializer {

    private final TProtocol protocol;
    private final TMemoryInputTransport transport;

    public PrefixedDeserializer() {
      transport = new TMemoryInputTransport();
      protocol = new TCompactProtocol(transport);
    }

    /**
     * Deserialize the Thrift object from a byte array.
     *
     * @param base The object to read into
     * @param bytes The array to read from
     */
    public void deserialize(TBase base, byte[] bytes) throws TException {
      try {
        if (bytes.length == 0) {
          return;
        }
        if (bytes[0] == PrefixedSerializer.SECRET_BYTE) {
          if (bytes.length == 1) {
            throw new TException("Unknown prefixed protocol with byte length 1.");
          }
          switch (bytes[1]) {
            case PrefixedSerializer.COMPACT_PROTOCOL_BYTE:
              transport.reset(bytes, 2, bytes.length - 2);
              base.read(protocol);
              break;
            default:
              throw new TException("Unknown protocol with byte: " + bytes[1]);
          }
        } else {
          // Default to TBinaryProtocol decoder.
          getInstance().decoder.get().deserialize(base, bytes);
        }
      } finally {
        transport.reset(null, 0, 0);
        protocol.reset();
      }
    }
  }

  private ThreadLocal<PrefixedSerializer> prefixedEncoder = new ThreadLocal<PrefixedSerializer>() {
    @Override
    protected PrefixedSerializer initialValue() {
      return new PrefixedSerializer();
    }
  };

  private ThreadLocal<PrefixedDeserializer> prefixedDecoder =
      new ThreadLocal<PrefixedDeserializer>() {
        @Override
        protected PrefixedDeserializer initialValue() {
          return new PrefixedDeserializer();
        }
      };

  private ThreadLocal<TSerializer> encoder = new ThreadLocal<TSerializer>() {
    @Override
    protected TSerializer initialValue() {
      return new TSerializer(new TBinaryProtocol.Factory());
    }
  };

  private ThreadLocal<TSerializer> encoderCompact = new ThreadLocal<TSerializer>() {
    @Override
    protected TSerializer initialValue() {
      return new TSerializer(new TCompactProtocol.Factory());
    }
  };

}
