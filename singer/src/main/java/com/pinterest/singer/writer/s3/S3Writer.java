package com.pinterest.singer.writer.s3;

import com.google.common.annotations.VisibleForTesting;

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.LogStreamWriter;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.common.errors.LogStreamWriterException;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.LogMessageAndPosition;
import com.pinterest.singer.thrift.configuration.S3WriterConfig;
import com.pinterest.singer.utils.SingerUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

/**
 * A LogStreamWriter for Singer that writes to S3 (writer.type=s3).
 * */
public class S3Writer implements LogStreamWriter {

  private static final String HOSTNAME = SingerUtils.HOSTNAME;
  private static final Logger LOG = LoggerFactory.getLogger(S3Writer.class);
  private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyyMMddHHmmssSSS");
  private final LogStream logStream;
  private final String logName;
  private final String BUFFER_DIR;
  private static final int BYTES_IN_MB = 1024 * 1024;
  private ObjectUploaderTask putObjectUploader;
  private S3Client s3Client;
  private final S3WriterConfig s3WriterConfig;

  // S3 information
  private String bucketName;
  private String keyFormat;

  // Disk-buffered file that will eventually be uploaded to S3 if size or time thresholds are met
  private BufferedOutputStream bufferedOutputStream;
  private File bufferFile;

  // Custom Thresholds
  private int maxFileSizeMB;
  private int minUploadTime;
  private int maxRetries;
  private Pattern filenamePattern;
  private List<String> fileNameTokens = new ArrayList<>();
  private boolean filenameParsingEnabled = false;

  // Timer for scheduling uploads
  private static ScheduledExecutorService fileUploadTimer;
  private Future<?> uploadFuture;
  private final Object objLock = new Object(); // used for synchronization locking

  static {
    ScheduledThreadPoolExecutor tmpTimer = new ScheduledThreadPoolExecutor(1);
    tmpTimer.setRemoveOnCancelPolicy(true);
    fileUploadTimer = tmpTimer;
  }

  public enum DefaultTokens {
    UUID("%UUID"),
    TIMESTAMP("%TIMESTAMP"),
    HOST("%HOST"),
    LOGNAME("%LOGNAME");
    private final String token;
    DefaultTokens(String token) {
      this.token = token;
    }
    public String getValue() {
      return token;
    }
  }

  /**
   * Constructs an S3Writer instance.
   *
   * @param logStream the LogStream associated with this writer
   * @param s3WriterConfig the S3WriterConfig containing configuration settings
   */
  public S3Writer(LogStream logStream, S3WriterConfig s3WriterConfig) {
    Preconditions.checkNotNull(logStream);
    Preconditions.checkNotNull(s3WriterConfig);

    this.logStream = logStream;
    this.logName = logStream.getSingerLog().getSingerLogConfig().getName();
    this.s3WriterConfig = s3WriterConfig;
    this.BUFFER_DIR = s3WriterConfig.getBufferDir();
    initialize();
  }

  // Static factory method for testing
  @VisibleForTesting
  public S3Writer(LogStream logStream, S3WriterConfig s3WriterConfig, S3Client s3Client,
                  ObjectUploaderTask putObjectUploader, String path) {
    Preconditions.checkNotNull(logStream);
    Preconditions.checkNotNull(s3WriterConfig);
    Preconditions.checkNotNull(s3Client);
    Preconditions.checkNotNull(putObjectUploader);
    Preconditions.checkNotNull(!Strings.isNullOrEmpty(path));

    this.BUFFER_DIR = path;
    this.logStream = logStream;
    this.logName = logStream.getSingerLog().getSingerLogConfig().getName();
    this.s3WriterConfig = s3WriterConfig;
    this.putObjectUploader = putObjectUploader;
    this.s3Client = s3Client;
    initialize();

  }

  private void initialize() {
    this.maxFileSizeMB = s3WriterConfig.getMaxFileSizeMB();
    this.minUploadTime = s3WriterConfig.getMinUploadTimeInSeconds();
    this.maxRetries = s3WriterConfig.getMaxRetries();

    if (s3WriterConfig.isSetFilenamePattern() && s3WriterConfig.isSetFilenameTokens()) {
      this.filenameParsingEnabled = true;
      this.filenamePattern = Pattern.compile(s3WriterConfig.getFilenamePattern());
      this.fileNameTokens = s3WriterConfig.getFilenameTokens();
    }

    // Create directory if it does not exist
    new File(BUFFER_DIR).mkdirs();
    try {
      resetBufferFile();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create buffer file", e);
    }

    this.keyFormat = s3WriterConfig.getKeyFormat();

    // Configure bucket name
    this.bucketName = s3WriterConfig.getBucket();
    if (this.bucketName == null) {
      throw new RuntimeException("Bucket name is not configured");
    }

    try {
      if (s3Client == null) {
        s3Client = S3Client.builder().build();
      }
      if (putObjectUploader == null) {
        putObjectUploader =
            new ObjectUploaderTask(s3Client, bucketName,
                ObjectCannedACL.fromValue(s3WriterConfig.getCannedAcl()), maxRetries);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public LogStream getLogStream() {
    return this.logStream;
  }

  @Override
  public boolean isAuditingEnabled() {
    return false;
  }

  @Override
  public boolean isCommittableWriter() {
    return true;
  }

  /**
   * Takes the fullPathPrefix and removes all slashes and replaces them with underscores.
   */
  public static String sanitizeFileName(String fullPathPrefix) {
    if (fullPathPrefix.startsWith("/")) {
      fullPathPrefix = fullPathPrefix.substring(1);
    }
    return fullPathPrefix.replace("/", "_");
  }

  /**
   * Starts the commit process, initializing the buffer file and scheduling an upload task if not
   * already scheduled.
   *
   * @param isDraining whether the system is in a draining state
   * @throws LogStreamWriterException if an error occurs while creating or writing to the buffer
   * file
   */
  @Override
  public synchronized void startCommit(boolean isDraining) throws LogStreamWriterException {
    try {
      if (!bufferFile.exists()) {
        bufferFile.createNewFile();
      }

      bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(bufferFile, true));

    } catch (IOException e) {
      throw new RuntimeException("Failed to create buffer file: " + bufferFile.getName(), e);
    }
    if (uploadFuture == null) {
      scheduleUploadTask();
    }
  }


  /**
   * Schedules a task to upload the buffer file at regular intervals.
   * If the buffer file exists and has data, it is renamed and a new buffer file is created.
   * The renamed file is then uploaded to S3.
   */
  private void scheduleUploadTask() {
    synchronized (objLock) {
      if (uploadFuture != null && !uploadFuture.isDone()) {
        LOG.info("An upload task is already scheduled or running");
        return;
      }
      uploadFuture = fileUploadTimer.schedule(() -> {
        try {
          synchronized (objLock) {
            if (bufferFile.length() > 0) {
              bufferedOutputStream.close();
              uploadDiskBufferedFileToS3();
            }
            uploadFuture = null;
          }
        } catch (Exception e) {
          LOG.error("Failed to upload buffer file to S3", e);
        }
      }, minUploadTime, TimeUnit.SECONDS);
    }
  }

  /**
   * Helper function that uploads the disk buffered file to s3
   * */
  private void uploadDiskBufferedFileToS3() throws IOException {
    File
        fileToUpload =
        new File(BUFFER_DIR, bufferFile.getName() + "." + FORMATTER.format(new Date()));
    String fileFormat = generateS3ObjectKey();
    try {
      Files.move(bufferFile.toPath(), fileToUpload.toPath());
      resetBufferFile();
      if (this.putObjectUploader.upload(fileToUpload, fileFormat)) {
        OpenTsdbMetricConverter.incr(SingerMetrics.S3_WRITER + "num_uploads", 1,
            "bucket=" + bucketName, "keyPrefix=" + keyFormat, "host=" + HOSTNAME,
            "logName=" + logName);
      } else {
        OpenTsdbMetricConverter.incr(SingerMetrics.S3_WRITER + "num_failed_uploads", 1,
            "bucket=" + bucketName, "keyPrefix=" + keyFormat, "host=" + HOSTNAME,
            "logName=" + logName);
      }
      fileToUpload.delete();
    } catch (IOException e) {
      LOG.error("Failed to rename buffer file " + bufferFile.getName(), e);
    }
  }

  /**
   * Writes a log message to the buffer file for the current commit.
   *
   * @param logMessageAndPosition the log message and its position
   * @param isDraining whether the system is in a draining state
   * @throws LogStreamWriterException if an error occurs while writing the log message
   */
  @Override
  public synchronized void writeLogMessageToCommit(LogMessageAndPosition logMessageAndPosition,
                                                   boolean isDraining)
      throws LogStreamWriterException {
    try {
      writeMessageToBuffer(logMessageAndPosition);
      OpenTsdbMetricConverter.incr(SingerMetrics.S3_WRITER + "num_messages_written",
          "bucket=" + bucketName, "host=" + HOSTNAME, "logName=" + logName);
    } catch (IOException e) {
      // TODO: Verify if this is retry is needed
      try {
        bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(bufferFile, true));
        writeMessageToBuffer(logMessageAndPosition);
      } catch (IOException ex) {
        LOG.error("Failed to close buffer writer", ex);
        throw new LogStreamWriterException("Failed to write log message to commit", e);
      }
    }
  }

  /**
   * This method writes the bytes of the log message to the buffered output stream
   * and adds a newline character after each log message. It then flushes the output stream
   * to ensure that the data is written to the buffer file.
   *
   * @param logMessageAndPosition the log message and its position to be written to the buffer
   * @throws IOException if an error occurs while writing to the buffer file
   */
  private void writeMessageToBuffer(LogMessageAndPosition logMessageAndPosition)
      throws IOException {
    byte[] logMessageBytes = logMessageAndPosition.logMessage.getMessage();
    bufferedOutputStream.write(logMessageBytes);
    bufferedOutputStream.write('\n'); // Add a newline character after each log message
    bufferedOutputStream.flush();
  }

  /**
   * Resets the buffer file by creating a new buffer file and buffered output stream.
   *
   * @throws IOException
   */
  private void resetBufferFile() throws IOException {
    String bufferFileName = sanitizeFileName(logStream.getFullPathPrefix()) + ".buffer.log";
    bufferFile = new File(BUFFER_DIR, bufferFileName);
    bufferFile.createNewFile();
    bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(bufferFile, true));
  }

  /**
   * Helper function to get the remaining part of the host name after the cluster prefix,
   * typically a UUID.
   */
  public static String extractHostSuffix(String inputStr) {
    String[] parts = inputStr.split("-");
    return parts[parts.length - 1];
  }

  private Matcher extractTokensFromFilename(String logFileName) {
    Matcher matcher = filenamePattern.matcher(logFileName);
    if (!matcher.matches()) {
      LOG.info("log file name: " + logFileName
          + " does not match the pattern: " + filenamePattern.toString());
      return null;
    }
    return matcher;
  }

  /**
   * Generates an S3 object key based on the configured key format. The key can contain tokens in the
   * format %{token} that will be replaced with the values extracted from the log filename based on
   * the regex pattern provided in filenamePattern using named regex groups.
   *
   * @return the generated S3 object key
   */
  public String generateS3ObjectKey() {
    String s3Key = keyFormat;
    Matcher matcher;
    // Replace default tokens
    // TODO: Implement a one pass replacement loop for all tokens if performance becomes an
    //  issue, for now this reads better.
    for (DefaultTokens token : DefaultTokens.values()) {
      switch (token) {
        case UUID:
          s3Key = s3Key.replace(token.getValue(), UUID.randomUUID().toString().substring(0, 8));
          break;
        case TIMESTAMP:
          s3Key = s3Key.replace(token.getValue(), FORMATTER.format(new Date()));
          break;
        case HOST:
          s3Key = s3Key.replace(token.getValue(), HOSTNAME);
          break;
        case LOGNAME:
          s3Key = s3Key.replace(token.getValue(), logName);
          break;
      }
    }
    // Replace named groups from filenamePattern
    if (filenameParsingEnabled) {
      if ((matcher = extractTokensFromFilename(logStream.getFileNamePrefix())) != null) {
        for (String token : fileNameTokens) {
          // Attempt to replace the token in filenamePattern with the matched value
          String matchedValue = matcher.group(token);
          if (matchedValue != null) {
            s3Key = s3Key.replace("%{" + token + "}", matchedValue);
          }
        }
      } else {
        // If there is no match we simply return the key without replacing any custom tokens
        LOG.warn("Filename parsing is enabled but filenamePattern provided: " + filenamePattern
            + " does not match the log file: " + logStream.getFileNamePrefix());
        OpenTsdbMetricConverter.incr(SingerMetrics.S3_WRITER + "no_filename_pattern_match", 1,
            "bucket=" + bucketName, "host=" + HOSTNAME, "logName=" + logName);
      }
    }

    // We append a random UUID to the key to avoid collisions
    // TODO: Provide configuration to disable this behavior if use case comes up
    s3Key += "." + UUID.randomUUID().toString().substring(0, 8);
    LOG.info("Generated S3 object key: " + s3Key);
    return s3Key;
  }

  /**
   * Ends the commit process by flushing the buffer and handling the buffer file if it exceeds
   * the maximum file size.
   *
   * @param numLogMessagesRead the number of log messages read
   * @param isDraining whether the system is in a draining state
   * @throws LogStreamWriterException if an error occurs while ending the commit
   */
  public synchronized void endCommit(int numLogMessagesRead, boolean isDraining)
      throws LogStreamWriterException {
    try {
      synchronized (objLock) {
        if (bufferFile.length() >= maxFileSizeMB * BYTES_IN_MB) {
          if (uploadFuture != null) {
            uploadFuture.cancel(true);
          }
          bufferedOutputStream.close();
          uploadDiskBufferedFileToS3();
          scheduleUploadTask();
        }
      }
    } catch (IOException e) {
      throw new LogStreamWriterException("Failed to end commit", e);
    }
  }

  /**
   * This method should not be used as it is Deprecated. Use comittable write method instead.
   *
   * @param messages The LogMessages to be written.
   * @throws LogStreamWriterException
   */
  @Deprecated
  public void writeLogMessages(List<LogMessage> messages) throws LogStreamWriterException {
    throw new LogStreamWriterException(
        "writeLogMessages is not supported. Use writeLogMessagesToCommit instead.");
  }


  /**
   * Closes the S3Writer, ensuring that remaining buffered log messages are safely uploaded to S3.
   *
   * This method synchronizes on {@code objLock} to ensure thread safety while performing the
   * following steps:
   * Increments the close counter metric in OpenTsdb.
   * If the buffer file has remaining data, it renames the file for upload, closes the buffer
   * stream,
   * and uploads the file to S3, recording relevant metrics.
   * Cancels the upload future task if it is not already done.
   * Attempts to close the buffered output stream if it is still open.
   * Closes the S3 client connection.
   * Any errors during these operations are logged accordingly.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    synchronized (objLock) {
      OpenTsdbMetricConverter.incr(SingerMetrics.S3_WRITER + "num_singer_close", 1,
          "bucket=" + bucketName, "keyPrefix=" + keyFormat, "host=" + HOSTNAME,
          "logName=" + logName);

      if (bufferFile.length() > 0) {
        try {
          bufferedOutputStream.close();
          uploadDiskBufferedFileToS3();
          bufferFile.delete();
        } catch (IOException e) {
          LOG.error("Failed to close bufferedWriter or upload buffer file: " + bufferFile.getName(),
              e);
        }
      }

      if (uploadFuture != null && !uploadFuture.isDone()) {
        uploadFuture.cancel(true);
      }

      try {
        if (bufferedOutputStream != null) {
          bufferedOutputStream.close();
        }
      } catch (IOException e) {
        LOG.error("Failed to close buffer writers", e);
      }

      if (s3Client != null) {
        s3Client.close();
      }
    }
  }
}