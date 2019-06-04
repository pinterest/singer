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
package com.pinterest.singer.utils;

import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.thrift.LogPosition;

import com.twitter.ostrich.stats.Stats;
import java.io.BufferedInputStream;
import java.io.File;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;

/**
 * Utilities for watermark.
 */
public final class WatermarkUtils {

  private static final Logger LOG = LoggerFactory.getLogger(WatermarkUtils.class);

  private WatermarkUtils() {
  }

  public static LogPosition loadCommittedPositionFromWatermark(String path)
      throws IOException, TException {
    InputStream inputStream = null;
    LogPosition position = new LogPosition();
    try {
      inputStream = new BufferedInputStream(new FileInputStream(path));
      TJSONProtocol protocol = new TJSONProtocol(new TIOStreamTransport(inputStream));
      position.read(protocol);
    } catch (FileNotFoundException e) {
      throw e;
    } catch (TException e) {
      debugCorruptedFile(path);
      LOG.error("Debug watermark error: see exception when load watermark");
      throw e;
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
    return position;
  }

  public static void saveCommittedPositionToWatermark(String path, LogPosition position)
      throws IOException, TException {
    String tmpWatermarkFile = path + ".tmp";
    FileOutputStream outputStream = null;
    TIOStreamTransport tStream = null;
    boolean success = false;
    try {
      outputStream = new FileOutputStream(tmpWatermarkFile);
      tStream = new TIOStreamTransport(outputStream);
      TJSONProtocol protocol = new TJSONProtocol(tStream);
      position.write(protocol);
    } finally {
      if (tStream != null) {
        try {
          tStream.flush();
          tStream.close();
          success = true;
        } catch (Exception e) {
          LOG.error("Failed to flush watermark file " + path + " at " + position, e);
        }
      }
      if (outputStream != null) {
        outputStream.close();
      }
    }
    if (success) {
      File watermarkFile = new File(tmpWatermarkFile);
      boolean renameResult = watermarkFile.renameTo(new File(path));
      if (!renameResult) {
        LOG.error("Failed to rename watermark file {} to {}", tmpWatermarkFile, watermarkFile);
        Stats.incr(SingerMetrics.WATERMARK_RENAME_FAILURE, 1);
      }
    } else {
      LOG.error("Failed to create tmp watermark file {}", tmpWatermarkFile);
      Stats.incr(SingerMetrics.WATERMARK_CREATION_FAILURE, 1);
    }
  }

  static void debugCorruptedFile(String srcFileName) {
    String dstFileName = srcFileName + ".debug";
    FileChannel dstChannel = null;
    FileChannel srcChannel = null;
    try {
      srcChannel = new FileInputStream(srcFileName).getChannel();
      dstChannel = new FileOutputStream(dstFileName).getChannel();
      dstChannel.transferFrom(srcChannel, 0, srcChannel.size());
      LOG.info("Debug watermark error: suspicious file copied to " + dstFileName);
    } catch (Exception e) {
      LOG.error("Debug watermark error: cannot copy the src file " + srcFileName, e);
    } finally {
      try {
        if (srcChannel != null) {
          srcChannel.close();
        }
        if (dstChannel != null) {
          dstChannel.close();
        }
      } catch (IOException ex) {
        LOG.error("Debug watermark error: Error when close the debug file channels", ex);
      }
    }
  }
}
