package com.pinterest.singer.reader.pooled;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.pinterest.singer.utils.SingerUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class PooledByteBufInputStreamTest {

  public final static String TEST_DIR = "target/pooled_tests";

  @After
  public void after() throws Exception {
    File baseDir = new File(TEST_DIR);
    SingerUtils.deleteRecursively(baseDir);
    Files.delete(baseDir.toPath());
  }

  @Test
  public void testBuffersAreClosed() throws Exception {
    File testDir = new File(TEST_DIR);
    testDir.mkdirs();
    int bufferCount = 10;
    List<ByteBuf> buffers = new ArrayList<>();
    List<PooledByteBufInputStream> inputStreams = new ArrayList<>();

    for (int i = 0; i < bufferCount; i++) {
      File file = new File(TEST_DIR + "/test" + i);
      file.createNewFile();
      ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer(1024, 1024);
      buffers.add(buffer);
      PooledByteBufInputStream
          inputStream =
          new PooledByteBufInputStream(new RandomAccessFile(file.getPath(), "r"), buffer);
      inputStreams.add(inputStream);
    }

    assertEquals(bufferCount, buffers.size());

    for (PooledByteBufInputStream inputStream : inputStreams) {
      inputStream.close();
    }

    int refCnt = 0;
    for (ByteBuf buffer : buffers) {
      refCnt += buffer.refCnt();
    }
    assertEquals(0, refCnt);
  }

  @Test
  public void testSimpleReadIntoBufferEOF() throws Exception {
    File testDir = new File(TEST_DIR);
    testDir.mkdirs();
    File testFile = new File(testDir + "/test");
    testFile.createNewFile();

    String value = "This is a simple read test";
    byte[] bytes = value.getBytes();
    Files.write(testFile.toPath(), bytes);
    RandomAccessFile file = new RandomAccessFile(testFile.getPath(), "r");
    PooledByteBufInputStream pooledByteBufInputStream = new PooledByteBufInputStream(file, PooledByteBufAllocator.DEFAULT.directBuffer(1024, 1024));
    byte[] buffer = new byte[1024];
    int read = pooledByteBufInputStream.read(buffer, 0, value.length());
    assertEquals(value.length(), read);
    assertEquals(value, new String(buffer, 0, read));
    assertTrue(pooledByteBufInputStream.isEOF());
  }
}
