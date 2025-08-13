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
package com.pinterest.singer.reader;

import com.pinterest.singer.SingerTestBase;
import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerLog;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.thrift.LogFile;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.thrift.configuration.TextLogMessageType;
import com.pinterest.singer.thrift.configuration.TextReaderConfig;
import com.pinterest.singer.utils.PatternCache;
import com.pinterest.singer.utils.SingerUtils;
import com.pinterest.singer.utils.TextLogger;

import org.apache.commons.io.FilenameUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class TestTextLogFileReaderFactory extends SingerTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp(); // Call parent setUp first

        SingerConfig singerConfig = new SingerConfig();
        SingerSettings.setSingerConfig(singerConfig);
    }

    @Test
    public void testFactoryUsesPatternCache() throws Exception {
        String path1 = FilenameUtils.concat(getTempPath(), "factory_test1.log");
        String path2 = FilenameUtils.concat(getTempPath(), "factory_test2.log");

        generateSampleMessagesToFile(path1, 50);
        generateSampleMessagesToFile(path2, 50);

        PatternCache.clearCache();

        // Create TextReaderConfig with common regex patterns
        TextReaderConfig readerConfig = new TextReaderConfig();
        readerConfig.setReaderBufferSize(8192);
        readerConfig.setMaxMessageSize(102400);
        readerConfig.setNumMessagesPerLogMessage(1);
        readerConfig.setMessageStartRegex("^.*$");
        readerConfig.setFilterMessageRegex("ERROR.*");
        readerConfig.setTextLogMessageType(TextLogMessageType.PLAIN_TEXT);
        readerConfig.setPrependTimestamp(false);
        readerConfig.setPrependHostname(false);
        readerConfig.setTrimTailingNewlineCharacter(true);

        long inode1 = SingerUtils.getFileInode(SingerUtils.getPath(path1));
        long inode2 = SingerUtils.getFileInode(SingerUtils.getPath(path2));
        LogFile logFile1 = new LogFile(inode1);
        LogFile logFile2 = new LogFile(inode2);
        LogStream logStream = new LogStream(new SingerLog(new SingerLogConfig()), "test");

        TextLogFileReaderFactory factory = new TextLogFileReaderFactory(readerConfig, null);

        // Create first reader through factory - should populate cache
        LogFileReader reader1 = factory.getLogFileReader(logStream, logFile1, path1, 0);
        int cacheAfterFirst = PatternCache.getCacheSize();
        assertTrue("Cache should have entries after first reader", cacheAfterFirst > 0);

        // Create second reader with same config - should reuse cached patterns
        LogFileReader reader2 = factory.getLogFileReader(logStream, logFile2, path2, 0);
        assertEquals("Cache size should not increase when reusing patterns",
                cacheAfterFirst, PatternCache.getCacheSize());

        // Verify readers work correctly
        assertNotNull("First reader should be created successfully", reader1);
        assertNotNull("Second reader should be created successfully", reader2);

        reader1.close();
        reader2.close();
    }

    @Test
    public void testFactoryWithDifferentConfigs() throws Exception {
        String path1 = FilenameUtils.concat(getTempPath(), "factory_diff1.log");
        String path2 = FilenameUtils.concat(getTempPath(), "factory_diff2.log");
        String path3 = FilenameUtils.concat(getTempPath(), "factory_diff3.log");

        generateSampleMessagesToFile(path1, 50);
        generateSampleMessagesToFile(path2, 50);
        generateSampleMessagesToFile(path3, 50);

        PatternCache.clearCache();

        long inode1 = SingerUtils.getFileInode(SingerUtils.getPath(path1));
        long inode2 = SingerUtils.getFileInode(SingerUtils.getPath(path2));
        long inode3 = SingerUtils.getFileInode(SingerUtils.getPath(path3));
        LogFile logFile1 = new LogFile(inode1);
        LogFile logFile2 = new LogFile(inode2);
        LogFile logFile3 = new LogFile(inode3);
        LogStream logStream = new LogStream(new SingerLog(new SingerLogConfig()), "test");

        // Config 1
        TextReaderConfig config1 = createBasicTextConfig();
        config1.setMessageStartRegex("^\\d{4}-\\d{2}-\\d{2}.*");
        TextLogFileReaderFactory factory1 = new TextLogFileReaderFactory(config1, null);

        // Config 2 - different regex
        TextReaderConfig config2 = createBasicTextConfig();
        config2.setMessageStartRegex("^ERROR.*");
        TextLogFileReaderFactory factory2 = new TextLogFileReaderFactory(config2, null);

        // Config 3 - same as config 1 (should reuse cache)
        TextReaderConfig config3 = createBasicTextConfig();
        config3.setMessageStartRegex("^\\d{4}-\\d{2}-\\d{2}.*");
        TextLogFileReaderFactory factory3 = new TextLogFileReaderFactory(config3, null);

        LogFileReader reader1 = factory1.getLogFileReader(logStream, logFile1, path1, 0);
        int cacheAfterFirst = PatternCache.getCacheSize();

        LogFileReader reader2 = factory2.getLogFileReader(logStream, logFile2, path2, 0);
        int cacheAfterSecond = PatternCache.getCacheSize();
        assertTrue("Cache should grow with different regex", cacheAfterSecond > cacheAfterFirst);

        LogFileReader reader3 = factory3.getLogFileReader(logStream, logFile3, path3, 0);
        int cacheAfterThird = PatternCache.getCacheSize();
        assertEquals("Cache should not grow when reusing regex", cacheAfterSecond, cacheAfterThird);

        reader1.close();
        reader2.close();
        reader3.close();
    }

    private TextReaderConfig createBasicTextConfig() {
        TextReaderConfig config = new TextReaderConfig();
        config.setReaderBufferSize(8192);
        config.setMaxMessageSize(102400);
        config.setNumMessagesPerLogMessage(1);
        config.setTextLogMessageType(TextLogMessageType.PLAIN_TEXT);
        config.setPrependTimestamp(false);
        config.setPrependHostname(false);
        config.setTrimTailingNewlineCharacter(true);
        return config;
    }

    private void generateSampleMessagesToFile(String path, int numMessages) throws FileNotFoundException, IOException {
        TextLogger logger = new TextLogger(path);
        for (int i = 0; i < numMessages; i++) {
            StringBuilder builder = new StringBuilder();
            for (int j = 0; j < ThreadLocalRandom.current().nextInt(5, 10); j++) {
                builder.append(UUID.randomUUID());
            }
            builder.append('\n');
            logger.logText(builder.toString());
        }
    }
}
