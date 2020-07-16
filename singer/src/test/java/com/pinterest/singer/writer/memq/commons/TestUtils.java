package com.pinterest.singer.writer.memq.commons;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.function.BiFunction;

import com.codahale.metrics.MetricRegistry;
import com.pinterest.singer.writer.memq.commons.serde.ByteArrayDeserializer;
import com.pinterest.singer.writer.memq.producer.MemqProducer;
import com.pinterest.singer.writer.memq.producer.http.HttpWriteTaskRequest;

public class TestUtils {

  public static byte[] getMemqBatchData(String baseLogMessage,
                                        BiFunction<String, Integer, byte[]> getLogMessageBytes,
                                        int logMessageCount,
                                        int msgs,
                                        Compression compression) throws Exception {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    for (int l = 0; l < msgs; l++) {
      Semaphore maxRequestLock = new Semaphore(1);
      HttpWriteTaskRequest task = new HttpWriteTaskRequest("xyz", 1L, compression, maxRequestLock,
          true, 1024 * 1024, 100, null);
      for (int k = 0; k < logMessageCount; k++) {
        byte[] bytes = getLogMessageBytes.apply(baseLogMessage, k);
        MemqProducer.writeMemqLogMessage(null, bytes, task);
      }
      task.markReady();
      task.getOutputStream().close();
      byte[] rawData = task.getByteArrays();
      os.write(rawData);
    }
    os.close();
    return os.toByteArray();
  }

  public static MemqLogMessageIterator<byte[], byte[]> getDummyIterator(String baseLogMessage,
                                                                        BiFunction<String, Integer, byte[]> getLogMessageBytes,
                                                                        int logMessageCount,
                                                                        int msgs,
                                                                        Compression compression) throws Exception {
    byte[] rawData = getMemqBatchData(baseLogMessage, getLogMessageBytes, logMessageCount, msgs,
        compression);

    return new MemqLogMessageIterator<>(ByteBuffer.wrap(rawData), null, new ByteArrayDeserializer(),
        new ByteArrayDeserializer(), new MetricRegistry());
  }
}