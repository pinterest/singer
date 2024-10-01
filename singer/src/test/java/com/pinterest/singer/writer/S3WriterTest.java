package com.pinterest.singer.writer;

import com.pinterest.singer.SingerTestBase;
import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerLog;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.LogMessageAndPosition;
import com.pinterest.singer.thrift.configuration.S3WriterConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.utils.SingerUtils;
import com.pinterest.singer.writer.s3.ObjectUploaderTask;
import com.pinterest.singer.writer.s3.S3Writer;
import com.pinterest.singer.writer.s3.S3Writer.DefaultTokens;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;

import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class S3WriterTest extends SingerTestBase {

  @Mock
  private S3Client mockS3Client;

  @Mock
  private ObjectUploaderTask mockObjectUploaderTask;

  private S3Writer s3Writer;
  private SingerLog singerLog;
  private LogStream logStream;
  private S3WriterConfig s3WriterConfig;
  private String tempPath;

  @Before
  public void setUp() {
    // set hostname
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
    s3Writer =
        new S3Writer(logStream, s3WriterConfig, mockS3Client, mockObjectUploaderTask, tempPath);
  }

  @After
  public void tearDown() throws IOException {
    File testBaseDir = new File(tempPath);
    if (testBaseDir.exists()) {
      FileUtils.deleteDirectory(testBaseDir);
    }
  }


  @Test
  public void testSanitizeFileName() {
    String fullPathPrefix = "/var/logs/app";
    String expected = "var_logs_app";
    String result = s3Writer.sanitizeFileName(fullPathPrefix);
    assertEquals(expected, result);

    fullPathPrefix = "var/logs/app";
    expected = "var_logs_app";
    result = s3Writer.sanitizeFileName(fullPathPrefix);
    assertEquals(expected, result);

    fullPathPrefix = "/var/logs/app/";
    expected = "var_logs_app_";
    result = s3Writer.sanitizeFileName(fullPathPrefix);
    assertEquals(expected, result);

    fullPathPrefix = "/";
    expected = "";
    result = s3Writer.sanitizeFileName(fullPathPrefix);
    assertEquals(expected, result);

    fullPathPrefix = "";
    expected = "";
    result = s3Writer.sanitizeFileName(fullPathPrefix);
    assertEquals(expected, result);
  }

  @Test
  public void testExtractHostSuffix() {
    String hostname = "app-server-12345";
    String expected = "12345";
    String result = S3Writer.extractHostSuffix(hostname);
    assertEquals(expected, result);

    hostname = "app-12345";
    expected = "12345";
    result = S3Writer.extractHostSuffix(hostname);
    assertEquals(expected, result);

    hostname = "12345";
    expected = "12345";
    result = S3Writer.extractHostSuffix(hostname);
    assertEquals(expected, result);

    hostname = "app-server";
    expected = "server";
    result = S3Writer.extractHostSuffix(hostname);
    assertEquals(expected, result);

    hostname = "";
    expected = "";
    result = S3Writer.extractHostSuffix(hostname);
    assertEquals(expected, result);
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
    String
        bufferFileNamePrefix =
        s3Writer.sanitizeFileName(logStream.getFullPathPrefix()) + ".buffer.";
    File tmpDir = new File(tempPath);
    File bufferFile = null;
    File [] tmpFiles = tmpDir.listFiles();
    boolean bufferFileExists = false;
    for (File file : tmpFiles) {
      if (file.getName().startsWith(bufferFileNamePrefix)) {
        bufferFileExists = true;
        bufferFile = file;
        break;
      }
    }
    assertTrue(bufferFileExists);
    String content = new String(Files.readAllBytes(bufferFile.toPath()));
    assertTrue(content.contains("test message"));
  }

  @Test
  public void testUploadToS3WhenSizeThresholdMet() throws Exception {
    // Prepare log message
    LogMessage
        logMessage =
        new LogMessage(ByteBuffer.wrap(new byte[1024 * 1024])); // simulate 1MB message
    LogMessageAndPosition logMessageAndPosition = new LogMessageAndPosition(logMessage, null);

    // Mock upload behavior
    when(mockObjectUploaderTask.upload(any(File.class), anyString())).thenReturn(true);

    // Write log messages to commit
    s3Writer.startCommit(false);
    for (int i = 0; i < 51; i++) { // Write enough to exceed the threshold
      s3Writer.writeLogMessageToCommit(logMessageAndPosition, false);
    }
    s3Writer.endCommit(2, false);

    // Verify upload was called
    verify(mockObjectUploaderTask, atLeastOnce()).upload(any(File.class), anyString());
  }

  @Test
  public void testUploadIsScheduled() throws Exception {
    // Prepare log message
    ByteBuffer messageBuffer = ByteBuffer.wrap(new byte[1024]); // simulate 1KB message
    LogMessage logMessage = new LogMessage(messageBuffer);
    LogMessageAndPosition logMessageAndPosition = new LogMessageAndPosition(logMessage, null);

    // Mock upload behavior
    when(mockObjectUploaderTask.upload(any(File.class), anyString())).thenReturn(true);

    // Write log messages to commit
    s3Writer.startCommit(false);
    s3Writer.writeLogMessageToCommit(logMessageAndPosition, false);

    // Simulate passage of time and scheduled upload
    Thread.sleep((s3WriterConfig.getMinUploadTimeInSeconds() + 2) * 1000);

    s3Writer.endCommit(1, false);

    // Verify upload was called
    verify(mockObjectUploaderTask, atLeastOnce()).upload(any(File.class), anyString());
  }

  @Test
  public void testS3ObjectKeyGeneration() {
    // Custom and default tokens used
    String
        keyFormat =
        "my-path/%{namespace}/" + DefaultTokens.LOGNAME.getValue() + "/%{filename}-%{index}."
            + DefaultTokens.TIMESTAMP.getValue();
    logStream = new LogStream(singerLog, "my_namespace-test_log.0");
    s3WriterConfig = new S3WriterConfig();
    s3WriterConfig.setKeyFormat(keyFormat);
    s3WriterConfig.setBucket("bucket-name");
    s3WriterConfig.setFilenamePattern("(?<namespace>[^-]+)-(?<filename>[^.]+)\\.(?<index>\\d+)");
    s3WriterConfig.setFilenameTokens(Arrays.asList("namespace", "filename", "index"));
    s3Writer =
        new S3Writer(logStream, s3WriterConfig, mockS3Client, mockObjectUploaderTask, tempPath);
    // Check key prefix
    String[] objectKeyParts = s3Writer.generateS3ObjectKey().split("/");
    assertEquals(4, objectKeyParts.length);
    assertEquals("my-path", objectKeyParts[0]);
    assertEquals("my_namespace", objectKeyParts[1]);
    assertEquals(logStream.getSingerLog().getSingerLogConfig().getName(), objectKeyParts[2]);
    // Check last part of object key
    String[] keySuffixParts = objectKeyParts[3].split("\\.");
    assertEquals(3, keySuffixParts.length);
    assertEquals("test_log-0", keySuffixParts[0]);
    assertNotEquals(DefaultTokens.LOGNAME.getValue(), keySuffixParts[1]);
    // Custom tokens provided but filename pattern does not match
    s3WriterConfig.setFilenamePattern("(?<filename>[^.]+)\\.(?<index>\\d+).0");
    s3Writer =
        new S3Writer(logStream, s3WriterConfig, mockS3Client, mockObjectUploaderTask, tempPath);
    objectKeyParts = s3Writer.generateS3ObjectKey().split("/");
    assertEquals("%{namespace}", objectKeyParts[1]);
    keySuffixParts = objectKeyParts[3].split("\\.");
    assertEquals("%{filename}-%{index}", keySuffixParts[0]);
  }

  @Test
  public void testClose() throws Exception {
    // Prepare log message
    ByteBuffer messageBuffer = ByteBuffer.wrap("test message".getBytes());
    LogMessage logMessage = new LogMessage(messageBuffer);
    LogMessageAndPosition logMessageAndPosition = new LogMessageAndPosition(logMessage, null);

    // Write log message to commit
    s3Writer.startCommit(false);
    s3Writer.writeLogMessageToCommit(logMessageAndPosition, false);
    s3Writer.endCommit(1, false);

    // Call close
    s3Writer.close();

    // Verify that the buffer file was correctly handled
    String
        bufferFileName =
        s3Writer.sanitizeFileName(logStream.getFullPathPrefix()) + ".buffer.log";
    File bufferFile = new File(FilenameUtils.concat(tempPath, bufferFileName));
    assertTrue(!bufferFile.exists());
    assertEquals(0, bufferFile.length());
    verify(mockObjectUploaderTask, atLeastOnce()).upload(any(File.class), anyString());
  }
}
