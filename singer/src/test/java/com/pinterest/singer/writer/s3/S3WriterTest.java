package com.pinterest.singer.writer.s3;

import com.pinterest.singer.SingerTestBase;
import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerLog;
import com.pinterest.singer.common.errors.LogStreamWriterException;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.LogMessageAndPosition;
import com.pinterest.singer.thrift.configuration.S3WriterConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.utils.SingerUtils;
import com.pinterest.singer.writer.s3.S3Writer.DefaultTokens;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;

import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class S3WriterTest extends SingerTestBase {
  @Mock
  private S3Uploader mockS3Uploader;

  private S3Writer s3Writer;
  private SingerLog singerLog;
  private LogStream logStream;
  private S3WriterConfig s3WriterConfig;
  private String tempPath;

  @Before
  public void setUp() {
    // Set hostname
    SingerUtils.setHostname("localhost-dev", "-");

    // Initialize basics
    tempPath = getTempPath();
    SingerLogConfig singerLogConfig = createSingerLogConfig("testLog", tempPath);
    singerLog = new SingerLog(singerLogConfig);
    logStream = new LogStream(singerLog, "test_log");

    // Initialize S3WriterConfig
    s3WriterConfig = new S3WriterConfig();
    s3WriterConfig.setBucket("bucket-name");
    s3WriterConfig.setKeyFormat("key-prefix");
    s3WriterConfig.setMaxFileSizeMB(1);
    s3WriterConfig.setMinUploadTimeInSeconds(5);
    s3WriterConfig.setMaxRetries(3);

    // Initialize the S3Writer with mock dependencies
    s3Writer = new S3Writer(logStream, s3WriterConfig, mockS3Uploader, tempPath);
  }

  @After
  public void tearDown() throws IOException {
    File testBaseDir = new File(tempPath);
    if (testBaseDir.exists()) {
      FileUtils.deleteDirectory(testBaseDir);
    }
    // reset hostname
    SingerUtils.setHostname(SingerUtils.getHostname(), "-");
    s3Writer.close();
  }

  @Test
  public void testWriteLogMessageToCommit() throws Exception {
    // Prepare log message
    ByteBuffer messageBuffer = ByteBuffer.wrap("test message".getBytes());
    LogMessage logMessage = new LogMessage(messageBuffer);
    LogMessageAndPosition logMessageAndPosition = new LogMessageAndPosition(logMessage, null);

    // Write log message to commit
    s3Writer.startCommit(false);
    s3Writer.writeLogMessageToCommit(logMessageAndPosition, false);
    s3Writer.endCommit(1, false);

    // Verify that the messages are written to the buffer file
    File bufferFile = new File(tempPath + "/" + s3Writer.getBufferFileName());
    assertTrue(bufferFile.exists());
    String content = new String(Files.readAllBytes(bufferFile.toPath()));
    assertTrue(content.contains("test message"));
  }

  @Test
  public void testSmallMessagesDoNotTriggerSizeBasedUploads() throws Exception {
    // Configure large buffer to ensure small messages don't trigger upload
    s3WriterConfig.setMaxFileSizeMB(10); // 10MB limit
    s3Writer = new S3Writer(logStream, s3WriterConfig, mockS3Uploader, tempPath);

    // Prepare small message that won't exceed threshold
    ByteBuffer messageBuffer = ByteBuffer.wrap("small test message".getBytes());
    LogMessage logMessage = new LogMessage(messageBuffer);
    LogMessageAndPosition logMessageAndPosition = new LogMessageAndPosition(logMessage, null);

    // Write small message - should not trigger size-based upload
    s3Writer.startCommit(false);
    s3Writer.writeLogMessageToCommit(logMessageAndPosition, false);
    s3Writer.endCommit(1, false);

    verify(mockS3Uploader, never()).upload(any(S3ObjectUpload.class));

    File bufferFile = new File(tempPath + "/" + s3Writer.getBufferFileName());
    assertTrue("Buffer should contain data", bufferFile.exists() && bufferFile.length() > 0);
  }



  @Test
  public void testResumeFromExistingBufferFileAfterCrash() throws Exception {
    // Simulate crash scenario: write data but don't close properly (no upload)
    ByteBuffer messageBuffer = ByteBuffer.wrap("This is message 1".getBytes());
    LogMessage logMessage = new LogMessage(messageBuffer);
    LogMessageAndPosition logMessageAndPosition = new LogMessageAndPosition(logMessage, null);

    s3Writer.startCommit(false);
    s3Writer.writeLogMessageToCommit(logMessageAndPosition, false);
    s3Writer.endCommit(1, false);

    // Verify buffer file exists with first message
    File bufferFile = new File(tempPath + "/" + s3Writer.getBufferFileName());
    assertTrue(bufferFile.exists());
    assertTrue(bufferFile.length() > 0);
    String fileName = bufferFile.getName();

    // Simulate crash - skip calling close()

    // Create new S3Writer to simulate restart after crash
    // It should find and resume from the existing buffer file
    S3Writer newS3Writer = new S3Writer(logStream, s3WriterConfig, mockS3Uploader, tempPath);

    // Write another message - should append to existing buffer
    messageBuffer = ByteBuffer.wrap(" This is message 2".getBytes());
    logMessage = new LogMessage(messageBuffer);
    logMessageAndPosition = new LogMessageAndPosition(logMessage, null);

    newS3Writer.startCommit(false);
    newS3Writer.writeLogMessageToCommit(logMessageAndPosition, false);
    newS3Writer.endCommit(1, false);

    // Verify it's using the same buffer file (resume scenario)
    File resumedBufferFile = new File(tempPath + "/" + newS3Writer.getBufferFileName());
    assertEquals("Should resume using same buffer file", fileName, resumedBufferFile.getName());

    // Verify both messages are in the buffer
    String content = new String(Files.readAllBytes(resumedBufferFile.toPath()));
    assertTrue("Buffer should contain first message", content.contains("This is message 1"));
    assertTrue("Buffer should contain second message", content.contains(" This is message 2"));

    newS3Writer.close();
  }

  @Test
  public void testObjectKeyGeneration() {
    // Custom and default tokens used
    String
        keyFormat =
        "my-path/%{namespace}/{{" + DefaultTokens.LOGNAME
            + "}}/%{filename}-%{index}.{{S}}";
    logStream = new LogStream(singerLog, "my_namespace-test_log.0");
    s3WriterConfig = new S3WriterConfig();
    s3WriterConfig.setKeyFormat(keyFormat);
    s3WriterConfig.setBucket("bucket-name");
    s3WriterConfig.setFilenamePattern("(?<namespace>[^-]+)-(?<filename>[^.]+)\\.(?<index>\\d+)");
    s3WriterConfig.setFilenameTokens(Arrays.asList("namespace", "filename", "index"));
    s3Writer = new S3Writer(logStream, s3WriterConfig, mockS3Uploader, tempPath);

    // Check key prefix
    String[] objectKeyParts = s3Writer.generateS3ObjectKey().split("/");
    assertEquals(4, objectKeyParts.length);
    assertEquals("my-path", objectKeyParts[0]);
    assertEquals("my_namespace", objectKeyParts[1]);
    assertEquals(logStream.getSingerLog().getSingerLogConfig().getName(), objectKeyParts[2]);

    // Check last part of object key
    String[] keySuffixParts = objectKeyParts[3].split("\\.");
    assertEquals(2, keySuffixParts.length);
    assertEquals("test_log-0", keySuffixParts[0]);
    assertNotEquals("{{S}}", keySuffixParts[1]);
    assertEquals(2, keySuffixParts[1].length());

    // Custom tokens provided but filename pattern does not match
    s3WriterConfig.setFilenamePattern("(?<filename>[^.]+)\\.(?<index>\\d+).0");
    s3Writer = new S3Writer(logStream, s3WriterConfig, mockS3Uploader, tempPath);
    objectKeyParts = s3Writer.generateS3ObjectKey().split("/");
    assertEquals("%{namespace}", objectKeyParts[1]);
    keySuffixParts = objectKeyParts[3].split("\\.");
    assertEquals("%{filename}-%{index}", keySuffixParts[0]);

    // Custom tokens used but with typos in format
    // Final result should be: my-path/%{{namespace}}/%testLog/%test_log/0%/<seconds>}.<uuid>
    keyFormat =
        "my-path/%{{namespace}}/%{{" + DefaultTokens.LOGNAME
            + "}}/%%{filename}/%{index}%/{{S}}}";
    s3WriterConfig.setKeyFormat(keyFormat);
    s3WriterConfig.setFilenamePattern("(?<namespace>[^-]+)-(?<filename>[^.]+)\\.(?<index>\\d+)");
    s3Writer = new S3Writer(logStream, s3WriterConfig, mockS3Uploader, tempPath);
    objectKeyParts = s3Writer.generateS3ObjectKey().split("/");
    assertEquals(6, objectKeyParts.length);
    assertEquals("%{{namespace}}", objectKeyParts[1]);
    assertEquals("%" + logStream.getSingerLog().getSingerLogConfig().getName(), objectKeyParts[2]);
    assertEquals("%test_log", objectKeyParts[3]);
    assertEquals("0%", objectKeyParts[4]);
    assertEquals(3, objectKeyParts[5].split("\\.")[0].length());

    // Test with matchAbsolutePath enabled
    keyFormat =
        "my-path/%{namespace}/{{" + DefaultTokens.LOGNAME
            + "}}/%{filename}-%{index}.{{S}}";
    s3WriterConfig.setKeyFormat(keyFormat);
    s3WriterConfig.setFilenamePattern(tempPath + "/(?<namespace>[^-]+)-(?<filename>[^.]+)\\.(?<index>\\d+)");
    s3WriterConfig.setMatchAbsolutePath(true);
    s3Writer = new S3Writer(logStream, s3WriterConfig, mockS3Uploader, tempPath);
    objectKeyParts = s3Writer.generateS3ObjectKey().split("/");
    assertEquals(4, objectKeyParts.length);
  }

  @Test
  public void testTimedUploadSchedulerWithMultipleLogStreams() throws Exception {
    s3WriterConfig.setMinUploadTimeInSeconds(1);
    s3WriterConfig.setMaxFileSizeMB(100);

    LogStream logStream1 = new LogStream(singerLog, "test_log_1");
    LogStream logStream2 = new LogStream(singerLog, "test_log_2");
    LogStream logStream3 = new LogStream(singerLog, "test_log_3");

    S3Writer writer1 = new S3Writer(logStream1, s3WriterConfig, mockS3Uploader, tempPath);
    S3Writer writer2 = new S3Writer(logStream2, s3WriterConfig, mockS3Uploader, tempPath);
    S3Writer writer3 = new S3Writer(logStream3, s3WriterConfig, mockS3Uploader, tempPath);

    try {
      when(mockS3Uploader.upload(any(S3ObjectUpload.class))).thenReturn(true);

      // Write small data to each stream (not enough to trigger size-based upload)
      ByteBuffer messageBuffer = ByteBuffer.wrap("scheduler test data".getBytes());
      LogMessage logMessage = new LogMessage(messageBuffer);
      LogMessageAndPosition logMessageAndPosition = new LogMessageAndPosition(logMessage, null);

      writer1.startCommit(false);
      writer1.writeLogMessageToCommit(logMessageAndPosition, false);
      writer1.endCommit(1, false);

      Thread.sleep(300);
      writer2.startCommit(false);
      writer2.writeLogMessageToCommit(logMessageAndPosition, false);
      writer2.endCommit(1, false);

      Thread.sleep(300);
      writer3.startCommit(false);
      writer3.writeLogMessageToCommit(logMessageAndPosition, false);
      writer3.endCommit(1, false);

      File bufferFile1 = new File(tempPath + "/" + writer1.getBufferFileName());
      File bufferFile2 = new File(tempPath + "/" + writer2.getBufferFileName());
      File bufferFile3 = new File(tempPath + "/" + writer3.getBufferFileName());

      assertTrue("Buffer 1 should contain data", bufferFile1.exists() && bufferFile1.length() > 0);
      assertTrue("Buffer 2 should contain data", bufferFile2.exists() && bufferFile2.length() > 0);
      assertTrue("Buffer 3 should contain data", bufferFile3.exists() && bufferFile3.length() > 0);

      long initialSize1 = bufferFile1.length();
      long initialSize2 = bufferFile2.length();
      long initialSize3 = bufferFile3.length();

      Thread.sleep(2500); // 2.5 seconds should be enough

      verify(mockS3Uploader, atLeastOnce()).upload(any(S3ObjectUpload.class));

      long finalSize1 = bufferFile1.exists() ? bufferFile1.length() : 0;
      long finalSize2 = bufferFile2.exists() ? bufferFile2.length() : 0;
      long finalSize3 = bufferFile3.exists() ? bufferFile3.length() : 0;

      assertTrue("Buffer 1 should be cleared after scheduled upload", finalSize1 < initialSize1);
      assertTrue("Buffer 2 should be cleared after scheduled upload", finalSize2 < initialSize2);
      assertTrue("Buffer 3 should be cleared after scheduled upload", finalSize3 < initialSize3);

    } finally {
      // Clean up
      writer1.close();
      writer2.close();
      writer3.close();
    }
  }

  @Test
  public void testExceptionHandlingPreservesBufferOnFailure() throws Exception {
    s3WriterConfig.setMaxFileSizeMB(1);
    s3Writer = new S3Writer(logStream, s3WriterConfig, mockS3Uploader, tempPath);

    ByteBuffer messageBuffer = ByteBuffer.wrap("important data that must not be lost".getBytes());
    LogMessage logMessage = new LogMessage(messageBuffer);
    LogMessageAndPosition logMessageAndPosition = new LogMessageAndPosition(logMessage, null);

    when(mockS3Uploader.upload(any(S3ObjectUpload.class))).thenReturn(false);

    s3Writer.startCommit(false);
    s3Writer.writeLogMessageToCommit(logMessageAndPosition, false);
    s3Writer.endCommit(1, false);

    File bufferFile = new File(tempPath + "/" + s3Writer.getBufferFileName());
    assertTrue("Buffer should contain data", bufferFile.length() > 0);
    String originalContent = new String(Files.readAllBytes(bufferFile.toPath()));
    long originalSize = bufferFile.length();

    try {
      byte[] moreData = new byte[2 * 1024 * 1024];
      Arrays.fill(moreData, (byte) 'X');
      ByteBuffer largeBuffer = ByteBuffer.wrap(moreData);
      LogMessage largeMessage = new LogMessage(largeBuffer);
      LogMessageAndPosition largeLogMessageAndPosition = new LogMessageAndPosition(largeMessage, null);

      s3Writer.startCommit(false);
      // This should trigger upload due to size, which will fail
      s3Writer.writeLogMessageToCommit(largeLogMessageAndPosition, false);

      fail("Expected LogStreamWriterException when upload fails and buffer would be full");
    } catch (LogStreamWriterException e) {
      // Expected - upload failed, exception propagated for batch retry
    }

    assertTrue("Buffer file should still exist after upload failure", bufferFile.exists());
    assertEquals("Buffer size should be unchanged after upload failure", originalSize, bufferFile.length());

    String preservedContent = new String(Files.readAllBytes(bufferFile.toPath()));
    assertEquals("Buffer content should be preserved after upload failure", originalContent, preservedContent);

    verify(mockS3Uploader, atLeastOnce()).upload(any(S3ObjectUpload.class));
  }

  @Test
  public void testBackpressurePreventsBatchWritesWhenBufferFull() throws Exception {
    s3WriterConfig.setMaxFileSizeMB(1); // 1MB limit
    s3Writer = new S3Writer(logStream, s3WriterConfig, mockS3Uploader, tempPath);

    when(mockS3Uploader.upload(any(S3ObjectUpload.class))).thenReturn(false);

    byte[] data = new byte[512 * 1024];
    Arrays.fill(data, (byte) 'A');
    ByteBuffer messageBuffer = ByteBuffer.wrap(data);
    LogMessage logMessage = new LogMessage(messageBuffer);
    LogMessageAndPosition logMessageAndPosition = new LogMessageAndPosition(logMessage, null);

    s3Writer.startCommit(false);
    s3Writer.writeLogMessageToCommit(logMessageAndPosition, false);
    s3Writer.endCommit(1, false);

    File bufferFile = new File(tempPath + "/" + s3Writer.getBufferFileName());
    assertTrue("Buffer should contain initial data", bufferFile.length() > 0);
    long initialBufferSize = bufferFile.length();

    byte[] moreData = new byte[600 * 1024];
    Arrays.fill(moreData, (byte) 'B');
    ByteBuffer largeBuffer = ByteBuffer.wrap(moreData);
    LogMessage largeMessage = new LogMessage(largeBuffer);
    LogMessageAndPosition largeLogMessageAndPosition = new LogMessageAndPosition(largeMessage, null);

    // This should trigger upload attempt, fail, and throw exception (blocking the batch)
    try {
      s3Writer.startCommit(false);
      s3Writer.writeLogMessageToCommit(largeLogMessageAndPosition, false);
      fail("Expected LogStreamWriterException when buffer would overflow and upload fails");
    } catch (LogStreamWriterException e) {
      // Expected - backpressure prevents buffer overflow
      assertTrue("Exception should mention upload failure",
          e.getMessage().contains("upload failed") || e.getMessage().contains("Buffer file S3 upload failed"));
    }

    verify(mockS3Uploader).upload(any(S3ObjectUpload.class));

    assertTrue("Buffer should still exist after failed upload", bufferFile.exists());
    assertEquals("Buffer size should be unchanged after failed write attempt",
        initialBufferSize, bufferFile.length());
    assertTrue("Buffer should preserve original data", bufferFile.length() > 0);
  }

  @Test
  public void testSuccessfulUploadCleansBufferAndResetsStream() throws Exception {
    s3WriterConfig.setMaxFileSizeMB(1);
    s3Writer = new S3Writer(logStream, s3WriterConfig, mockS3Uploader, tempPath);

    when(mockS3Uploader.upload(any(S3ObjectUpload.class))).thenReturn(true);

    byte[] largeData = new byte[2 * 1024 * 1024]; // 2MB message
    Arrays.fill(largeData, (byte) 'S');
    ByteBuffer messageBuffer = ByteBuffer.wrap(largeData);
    LogMessage logMessage = new LogMessage(messageBuffer);
    LogMessageAndPosition logMessageAndPosition = new LogMessageAndPosition(logMessage, null);

    s3Writer.startCommit(false);

    File bufferFile = new File(tempPath + "/" + s3Writer.getBufferFileName());
    assertTrue("Buffer file should exist after startCommit", bufferFile.exists());

    s3Writer.writeLogMessageToCommit(logMessageAndPosition, false);
    s3Writer.endCommit(1, false);

    verify(mockS3Uploader, atLeastOnce()).upload(any(S3ObjectUpload.class));

    File currentBufferFile = new File(tempPath + "/" + s3Writer.getBufferFileName());

    // Verify buffer was cleaned up (reset to empty)
    assertTrue("Buffer file should exist after successful upload (reset)", currentBufferFile.exists());
    assertEquals("Buffer should be empty after successful upload cleanup", 0, currentBufferFile.length());

    ByteBuffer newMessageBuffer = ByteBuffer.wrap("new data after reset".getBytes());
    LogMessage newLogMessage = new LogMessage(newMessageBuffer);
    LogMessageAndPosition newLogMessageAndPosition = new LogMessageAndPosition(newLogMessage, null);

    s3Writer.startCommit(false);
    s3Writer.writeLogMessageToCommit(newLogMessageAndPosition, false);
    s3Writer.endCommit(1, false);

    // Verify new data was written successfully
    assertTrue("Buffer should contain new data after reset", currentBufferFile.length() > 0);
    String content = new String(Files.readAllBytes(currentBufferFile.toPath()));
    assertTrue("Buffer should contain new message", content.contains("new data after reset"));
  }

  @Test
  public void testBufferFileRecoveryAfterRestartWithMultipleLogStreams() throws Exception {
    // Create two different log streams (simulating different log files)
    LogStream logStream1 = new LogStream(singerLog, "test_log_1");
    LogStream logStream2 = new LogStream(singerLog, "test_log_2");

    // Create S3Writers for each log stream and write data
    S3Writer writer1 = new S3Writer(logStream1, s3WriterConfig, mockS3Uploader, tempPath);
    S3Writer writer2 = new S3Writer(logStream2, s3WriterConfig, mockS3Uploader, tempPath);

    ByteBuffer messageBuffer = ByteBuffer.wrap("test message".getBytes());
    LogMessage logMessage = new LogMessage(messageBuffer);
    LogMessageAndPosition logMessageAndPosition = new LogMessageAndPosition(logMessage, null);

    // Write to both writers
    writer1.startCommit(false);
    writer1.writeLogMessageToCommit(logMessageAndPosition, false);
    writer1.endCommit(1, false);

    writer2.startCommit(false);
    writer2.writeLogMessageToCommit(logMessageAndPosition, false);
    writer2.endCommit(1, false);

    // Get buffer file names
    String bufferFile1Name = writer1.getBufferFileName();
    String bufferFile2Name = writer2.getBufferFileName();

    // Verify they have different buffer file names
    assertNotEquals("Different log streams should have different buffer files", bufferFile1Name, bufferFile2Name);

    // Close writers (simulating shutdown)
    writer1.close();
    writer2.close();

    // Verify buffer files exist
    File buffer1 = new File(tempPath + "/" + bufferFile1Name);
    File buffer2 = new File(tempPath + "/" + bufferFile2Name);
    assertTrue("Buffer file 1 should exist after close", buffer1.exists());
    assertTrue("Buffer file 2 should exist after close", buffer2.exists());

    // Simulate restart: create new S3Writers for the same log streams
    S3Writer newWriter1 = new S3Writer(logStream1, s3WriterConfig, mockS3Uploader, tempPath);
    S3Writer newWriter2 = new S3Writer(logStream2, s3WriterConfig, mockS3Uploader, tempPath);

    // Verify each writer found its own buffer file (not the other's)
    String recoveredBufferFile1Name = newWriter1.getBufferFileName();
    String recoveredBufferFile2Name = newWriter2.getBufferFileName();

    assertEquals("Writer 1 should recover its own buffer file", bufferFile1Name, recoveredBufferFile1Name);
    assertEquals("Writer 2 should recover its own buffer file", bufferFile2Name, recoveredBufferFile2Name);
    assertNotEquals("Recovered buffer files should still be different", recoveredBufferFile1Name, recoveredBufferFile2Name);

    newWriter1.close();
    newWriter2.close();
  }
}
