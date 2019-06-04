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

import com.pinterest.singer.SingerTestBase;
import com.pinterest.singer.thrift.LogFile;
import com.pinterest.singer.thrift.LogPosition;

import org.apache.commons.io.FilenameUtils;
import org.junit.Test;

public class WatermarkUtilsTest extends SingerTestBase {

  @Test
  public void testSaveAndLoad() throws Exception {
    String watermarkFilePath = FilenameUtils.concat(getTempPath(), ".watermark");
    LogPosition expectedPosition = new LogPosition(new LogFile(1234L), 1234L);
    WatermarkUtils.saveCommittedPositionToWatermark(watermarkFilePath, expectedPosition);
    LogPosition actualPosition = WatermarkUtils.loadCommittedPositionFromWatermark(watermarkFilePath);
    assertEquals(actualPosition, expectedPosition);
  }
}