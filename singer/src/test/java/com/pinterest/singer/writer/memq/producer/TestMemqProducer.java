package com.pinterest.singer.writer.memq.producer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.Semaphore;
import java.util.function.BiFunction;
import java.util.zip.CRC32;

import org.junit.Test;

import com.pinterest.singer.writer.memq.commons.Compression;
import com.pinterest.singer.writer.memq.commons.MemqLogMessage;
import com.pinterest.singer.writer.memq.commons.TestUtils;
import com.pinterest.singer.writer.memq.producer.http.HttpWriteTaskRequest;

public class TestMemqProducer {

  @Test
  public void testProducerMessageEncoding() throws IOException, InstantiationException,
                                            IllegalAccessException, IllegalArgumentException,
                                            InvocationTargetException, NoSuchMethodException,
                                            SecurityException {
    Semaphore maxRequestLock = new Semaphore(1);
    HttpWriteTaskRequest task = new HttpWriteTaskRequest("xyz", 1L, Compression.GZIP,
        maxRequestLock, true, 1024 * 1024, 100, null);
    int count = 100;
    OutputStream os = task.getOutputStream();
    String data = "xyzabcs";
    String ary = data;
    for (int i = 0; i < 2; i++) {
      ary += ary;
    }
    for (int k = 0; k < count; k++) {
      byte[] bytes = (ary + "i:" + k).getBytes();
      MemqProducer.writeMemqLogMessage(null, bytes, task);
    }
    task.markReady();
    os.close();
    byte[] buf = task.getByteArrays();
    ByteBuffer wrap = ByteBuffer.wrap(buf);

    // task.getHeader().getHeaderLength(), buf.length);
    System.out.println("Header length:" + task.getHeader().getHeaderLength());
    assertEquals("Header length didn't match in payload", task.getHeader().getHeaderLength(),
        wrap.getShort());
    assertEquals(task.getVersion(), wrap.getShort());
    short extraHeaderLength = wrap.getShort();
    assertEquals(0, extraHeaderLength);
    // skip testing extra content
    wrap.position(wrap.position() + extraHeaderLength);

    int crc = wrap.getInt();
    byte compression = wrap.get();
    assertEquals(1, compression);
    int messageCount = wrap.getInt();
    assertEquals(count, messageCount);
    int lengthOfPayload = wrap.getInt();
    ByteBuffer payload = wrap.slice();
    CRC32 crcCalc = new CRC32();
    crcCalc.update(payload);
    payload.rewind();

    byte[] outputPayload = new byte[lengthOfPayload];
    for (int i = 0; i < lengthOfPayload; i++) {
      outputPayload[i] = payload.get();
    }

    InputStream stream = new ByteArrayInputStream(outputPayload);
    for (Compression comp : Compression.values()) {
      if (comp.id == compression) {
        stream = comp.inputStream.getConstructor(InputStream.class).newInstance(stream);
        break;
      }
    }

    DataInputStream dis = new DataInputStream(stream);
    for (int k = 0; k < count; k++) {
      short internalFieldsLength = dis.readShort();
      assertEquals(8, internalFieldsLength);
      dis.read(new byte[internalFieldsLength]);
      int headerLength = dis.readInt();
      assertEquals(0, headerLength);
      int length = dis.readInt();
      byte[] b = new byte[length];
      dis.readFully(b);
      assertEquals(crc, (int) crcCalc.getValue());
      assertArrayEquals((ary + "i:" + k).getBytes(), b);
    }
  }

  @Test
  public void testProduceConsumeProtocolCompatibility() throws Exception {
    // write data using producer
    int count = 10241;
    for (Compression c : Compression.values()) {
      String data = "xyza33245245234534bcs";
      String ary = data;
      for (int i = 0; i < 2; i++) {
        ary += ary;
      }
      BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> (base + k).getBytes();
      String baseLogMessage = ary + "i:";
      Iterator<MemqLogMessage<byte[], byte[]>> iterator = TestUtils.getDummyIterator(baseLogMessage,
          getLogMessageBytes, count, 1, c);
      for (int i = 0; i < 50_001; i++) {
        assertTrue(iterator.hasNext()); // assert idempotence
      }
      int z = 0;
      while (iterator.hasNext()) {
        MemqLogMessage<byte[], byte[]> next = iterator.next();
        assertNull(next.getHeaders());
        assertEquals("z:" + z, ary + "i:" + z, new String(next.getValue())); // assert value equals
        z++;
      }
      assertEquals(count, z);
      assertFalse(iterator.hasNext());
    }
  }

}
