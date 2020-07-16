package com.pinterest.singer.writer.memq.commons;

import java.io.DataInputStream;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.zip.CRC32;

public class CommonUtils {

  /**
   * Validate whether CRC checksum for a batch matches the given headerCrc
   *
   * @param batch
   * @param headerCrc
   * @return
   */
  public static boolean crcChecksumMatches(byte[] batch, int headerCrc) {
    CRC32 crc = new CRC32();
    crc.update(batch);
    if ((int) crc.getValue() != headerCrc) {
      return false;
    }
    return true;
  }

  /**
   * Given a compression id and compressed InputStream, return an uncompressed
   * DataInputStream
   *
   * @param compression the compression id
   * @param original    the compressed InputStream
   * @return an uncompressed DataInputStream
   * @throws NoSuchMethodException
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   * @throws InstantiationException
   * @throws UnknownCompressionException
   */
  public static DataInputStream getUncompressedInputStream(byte compression,
                                                           InputStream original) throws NoSuchMethodException,
                                                                                 IllegalAccessException,
                                                                                 InvocationTargetException,
                                                                                 InstantiationException,
                                                                                 UnknownCompressionException {
    if (compression == 0) {
      return new DataInputStream(original);
    }
    for (Compression comp : Compression.values()) {
      if (comp.id == compression) {
        return new DataInputStream(
            comp.inputStream.getConstructor(InputStream.class).newInstance(original));
      }
    }
    throw new UnknownCompressionException("Compression id " + compression + " is not supported");
  }

}
