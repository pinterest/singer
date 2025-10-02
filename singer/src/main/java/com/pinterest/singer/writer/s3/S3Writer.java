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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.services.s3.S3Client;

/**
 * A LogStreamWriter for Singer that writes to S3 (writer.type=s3).
 * */
public class S3Writer implements LogStreamWriter {

  private static final String HOSTNAME = SingerUtils.HOSTNAME;
  private static final Logger LOG = LoggerFactory.getLogger(S3Writer.class);
  private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyyMMddHHmmssSSS");
  private static final int BYTES_IN_MB = 1024 * 1024;
  private static final int TIME_UPLOAD_SCHEDULER_THREAD_POOL_SIZE = 2;

  // Time-based upload scheduler
  private static final ScheduledExecutorService TIME_UPLOAD_SCHEDULER =
      Executors.newScheduledThreadPool(TIME_UPLOAD_SCHEDULER_THREAD_POOL_SIZE, new ThreadFactoryBuilder()
          .setNameFormat("S3Writer-TimeUpload-%d")
          .setDaemon(true)
          .build());

  private final Map<String, String> envMappings = System.getenv();
  private final LogStream logStream;
  private final String logName;
  private final String BUFFER_DIR;
  private S3Uploader s3Uploader;
  private final S3WriterConfig s3WriterConfig;

  // S3 information
  private String bucketName;
  private String keyFormat;

  // Disk-buffered file that will eventually be uploaded to S3 if size or time thresholds are met
  private BufferedOutputStream bufferedOutputStream;
  private File bufferFile;
  private volatile long bufferFileCreatedTimeMs = 0;

  // Custom Thresholds
  private int maxFileSizeMB;
  private int minUploadTime;
  private Pattern filenamePattern;
  private List<String> fileNameTokens = new ArrayList<>();
  private boolean filenameParsingEnabled = false;
  private boolean matchAbsolutePath;

  // Time-based upload management
  private Future<?> timeBasedUploadTask = null;

  private final Object objLock = new Object(); // used for synchronization locking

  public enum DefaultTokens {
    UUID,
    TIMESTAMP,
    HOST,
    LOGNAME;
  }

  public enum TriggerType {
    TIME,
    SIZE,
    CLOSE;
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
  public S3Writer(LogStream logStream, S3WriterConfig s3WriterConfig, S3Uploader s3Uploader,
      String path) {
    Preconditions.checkNotNull(logStream);
    Preconditions.checkNotNull(s3WriterConfig);
    Preconditions.checkNotNull(s3Uploader);
    Preconditions.checkNotNull(!Strings.isNullOrEmpty(path));

    this.BUFFER_DIR = path;
    this.logStream = logStream;
    this.logName = logStream.getSingerLog().getSingerLogConfig().getName();
    this.s3WriterConfig = s3WriterConfig;
    this.s3Uploader = s3Uploader;
    initialize();

  }

  private void initialize() {
    this.maxFileSizeMB = s3WriterConfig.getMaxFileSizeMB();
    this.minUploadTime = s3WriterConfig.getMinUploadTimeInSeconds();
    if (s3WriterConfig.isSetFilenamePattern() && s3WriterConfig.isSetFilenameTokens()) {
      this.filenameParsingEnabled = true;
      this.filenamePattern = Pattern.compile(s3WriterConfig.getFilenamePattern());
      this.fileNameTokens = s3WriterConfig.getFilenameTokens();
    }

    scheduleTimeBasedUploadCheck();

    // Create directory if it does not exist
    new File(BUFFER_DIR).mkdirs();
    try {
      // Look for existing buffer file first (on startup/new log stream)
      File existingBuffer = findExistingBufferFile();
      if (existingBuffer != null) {
        bufferFile = existingBuffer;
        bufferFileCreatedTimeMs = extractTimestampFromFilename(bufferFile.getName());
        if (bufferFileCreatedTimeMs == 0) {
          LOG.warn("Failed to extract timestamp from existing buffer file: {}, using current time", bufferFile.getName());
          bufferFileCreatedTimeMs = System.currentTimeMillis();
        }
        LOG.info("Found existing buffer file for log stream {}: {}, timestamp: {}",
            logName, bufferFile.getName(), bufferFileCreatedTimeMs);
        bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(bufferFile, true));
      } else {
        resetBufferFile();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to create buffer file", e);
    }

    this.keyFormat = s3WriterConfig.getKeyFormat();
    this.matchAbsolutePath = s3WriterConfig.isMatchAbsolutePath();

    // Configure bucket name
    this.bucketName = s3WriterConfig.getBucket();
    if (this.bucketName == null) {
      throw new RuntimeException("Bucket name is not configured");
    }

    try {
      if (s3Uploader == null) {
        Class<?> clazz = Class.forName(s3WriterConfig.getUploaderClass());
        s3Uploader =
            (S3Uploader) clazz.getConstructor(S3WriterConfig.class, S3Client.class)
                .newInstance(s3WriterConfig, S3ClientManager.getInstance()
                    .get(s3WriterConfig.getRegion()));
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
   * Get or construct buffer file name based on the log stream name, absolute path hash, and timestamp.
   * The buffer file naming convention is "log_name.absolute_path_hash.timestamp".
   *
   * @return the buffer file name
   */
  public String getBufferFileName() {
    if (bufferFile != null) {
      return bufferFile.getName();
    }
    return generateBufferFileName(System.currentTimeMillis());
  }

  private String generateBufferFileName(long timestamp) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(logStream.getFullPathPrefix().getBytes(StandardCharsets.UTF_8));
      StringBuilder hashedPath = new StringBuilder(64);
      for (byte b : hash) {
        String hex = Integer.toHexString(0xff & b);
        // handle single digit values
        if (hex.length() == 1) hashedPath.append('0');
        hashedPath.append(hex);
      }
      return logName + "." + hashedPath + "." + timestamp;
    } catch (Exception e) {
      if (e instanceof NoSuchAlgorithmException) {
        LOG.error("Failed to generate hash for log stream {} and absolute path {}", logName,
            logStream.getFullPathPrefix(), e);
      }
      throw new RuntimeException("Failed to generate buffer file name", e);
    }
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
        // Buffer file missing - create a new one (recovery case)
        resetBufferFile();
      }
      // If buffer exists, bufferedOutputStream should already be set up correctly
    } catch (IOException e) {
      throw new RuntimeException("Failed to create buffer file: " + getBufferFileName(), e);
    }
  }

  /**
   * Schedules a periodic task to check for time-based uploads.
   * Runs every 1/4 of the minUploadTime to ensure timely uploads even when no new messages arrive.
   */
  private void scheduleTimeBasedUploadCheck() {
    if (minUploadTime <= 0) {
      return;
    }

    // Check every 1/4 of the upload time interval, minimum of 1 seconds
    long checkIntervalSeconds = Math.max(1, minUploadTime / 4);

    timeBasedUploadTask = TIME_UPLOAD_SCHEDULER.scheduleWithFixedDelay(() -> {
      try {
        synchronized (objLock) {
          // Check if buffer should be uploaded based on time
          if (bufferFile != null && bufferFile.exists() && bufferFile.length() > 0) {
            if (bufferFileCreatedTimeMs == 0) {
              LOG.warn("Buffer file creation time not set for {} - disabling time-based upload",
                  bufferFile.getName());
            } else {
              long currentTimeMs = System.currentTimeMillis();
              long ageInSeconds = (currentTimeMs - bufferFileCreatedTimeMs) / 1000;
              if (ageInSeconds >= minUploadTime) {
                LOG.info("Buffer file for log {} is {} seconds old, minUploadTime is {}", logName, ageInSeconds, minUploadTime);
                LOG.info("Periodic time-based upload triggered for log {}", logName);
                uploadDiskBufferedFileToS3(TriggerType.TIME);
              }
            }
          }
        }
      } catch (Exception e) {
        LOG.error("Error in time-based upload checker for log {}", logName, e);
      }
    }, checkIntervalSeconds, checkIntervalSeconds, TimeUnit.SECONDS);
  }

  /**
   * Uploads the disk buffered file to S3 and manages bufferedOutputStream lifecycle.
   *
   * This method handles the upload cycle:
   * 1. Closes the current bufferedOutputStream to ensure data is flushed to disk
   * 2. Uploads the buffer file to S3
   * 3. Cleans up the old buffer file and creates a new one
   * 4. Reopens the bufferedOutputStream for continued writing
   *
   * If upload fails, the buffer file remains intact for crash recovery.
   *
   * @param triggerType the type of trigger that initiated this upload (time, size, or close)
   * @throws LogStreamWriterException if S3 upload fails or stream management fails
   */
  private void uploadDiskBufferedFileToS3(TriggerType triggerType) throws LogStreamWriterException {
    // Close current stream to ensure all data is written to disk
    try {
      bufferedOutputStream.close();
    } catch (IOException e) {
      LOG.error("Failed to close bufferedOutputStream before S3 upload", e);
      throw new LogStreamWriterException("Cannot close buffer stream before upload", e);
    }

    String s3Key = generateS3ObjectKey();
    boolean uploadSuccess = this.s3Uploader.upload(new S3ObjectUpload(s3Key, bufferFile));

    if (!uploadSuccess) {
      LOG.error("S3 upload failed for buffer file {}", getBufferFileName());
      OpenTsdbMetricConverter.incr(SingerMetrics.S3_WRITER + "num_failed_uploads", 1,
          "bucket=" + bucketName, "host=" + HOSTNAME, "logName=" + logName, "trigger_type=" + triggerType.name());
      throw new LogStreamWriterException("Buffer file S3 upload failed for log stream " + logName);
    }

    OpenTsdbMetricConverter.incr(SingerMetrics.S3_WRITER + "num_uploads", 1,
        "bucket=" + bucketName, "host=" + HOSTNAME, "logName=" + logName, "trigger_type=" + triggerType.name());
    LOG.info("Successfully uploaded buffer file {}", getBufferFileName());

    // Always clean up buffer file and reopen stream after successful upload
    boolean deleted = bufferFile.delete();
    if (deleted) {
      OpenTsdbMetricConverter.incr(SingerMetrics.S3_WRITER + "buffer_file_delete", 1,
          "bucket=" + bucketName, "host=" + HOSTNAME, "logName=" + logName, "success=true");
      LOG.debug("Deleted buffer file after successful upload: {}", getBufferFileName());
    } else {
      OpenTsdbMetricConverter.incr(SingerMetrics.S3_WRITER + "buffer_file_delete", 1,
          "bucket=" + bucketName, "host=" + HOSTNAME, "logName=" + logName, "success=false");
      LOG.warn("Failed to delete buffer file after successful upload: {}", getBufferFileName());
    }

    // Only create new buffer and reopen stream if not during close
    if (triggerType != TriggerType.CLOSE) {
      try {
        resetBufferFile();
      } catch (IOException e) {
        LOG.error("Failed to reset buffer file after successful S3 upload of {}", getBufferFileName(), e);
        OpenTsdbMetricConverter.incr(SingerMetrics.S3_WRITER + "buffer_reset_failed", 1,
            "bucket=" + bucketName, "host=" + HOSTNAME, "logName=" + logName);
        throw new LogStreamWriterException("IO exception after successful S3 upload", e);
      }
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

    synchronized (objLock) {
      long currentBufferSize = bufferFile.length();
      long messageSize = logMessageAndPosition.logMessage.getMessage().length;
      long effectiveBufferSize = currentBufferSize + messageSize;

      // Check if buffer size threshold is exceeded
      if (effectiveBufferSize >= maxFileSizeMB * BYTES_IN_MB) {
        LOG.info("Buffer file size {} has exceeded the size threshold of {}, attempting S3 upload",
            effectiveBufferSize, maxFileSizeMB * BYTES_IN_MB);
        uploadDiskBufferedFileToS3(TriggerType.SIZE);
      }
    }

    try {
      byte[] logMessageBytes = logMessageAndPosition.logMessage.getMessage();
      bufferedOutputStream.write(logMessageBytes);
      // Don't flush after each message - defer to endCommit
      OpenTsdbMetricConverter.incr(SingerMetrics.S3_WRITER + "num_messages_written",
          "bucket=" + bucketName, "host=" + HOSTNAME, "logName=" + logName);
    } catch (IOException e) {
      LOG.error("Failed to write message to buffer file", e);
      throw new LogStreamWriterException("Failed to write log message to commit", e);
    }
  }

  /**
   * Resets the buffer file by creating a fresh one with new timestamp.
   * Used after successful uploads to start clean.
   *
   * @throws IOException
   */
  private void resetBufferFile() throws IOException {
    if (bufferFile != null && bufferFile.exists()) {
      LOG.warn("Buffer file still exists, skipping reset");
      return;
    }
    long currentTime = System.currentTimeMillis();
    String newFileName = generateBufferFileName(currentTime);
    bufferFile = new File(BUFFER_DIR, newFileName);
    bufferFileCreatedTimeMs = currentTime;
    if (bufferFile.createNewFile()) {
      LOG.info("Created new buffer file for log stream {}: {}", logName, newFileName);
    }
    bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(bufferFile, true));
  }

  /**
   * Find existing buffer file for this log stream.
   */
  private File findExistingBufferFile() {
    File bufferDir = new File(BUFFER_DIR);
    if (!bufferDir.exists()) {
      return null;
    }

    // Generate expected filename prefix
    // but without the timestamp part: logName.hash.
    String dummyFileName = generateBufferFileName(0); // Use timestamp 0 as placeholder
    String expectedPrefix = dummyFileName.substring(0, dummyFileName.lastIndexOf('.') + 1); // Keep "logName.hash."

    File[] existingFiles = bufferDir.listFiles((dir, name) -> name.startsWith(expectedPrefix));
    if (existingFiles == null || existingFiles.length == 0) {
      return null;
    }

    // If multiple files exist, use the one with latest timestamp from filename
    File bestMatch = null;
    long latestTimestamp = 0;

    for (File file : existingFiles) {
      long timestamp = extractTimestampFromFilename(file.getName());
      if (timestamp > latestTimestamp) {
        latestTimestamp = timestamp;
        bestMatch = file;
      }
    }
    return bestMatch;
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
   * Extract timestamp from buffer filename.
   * Expected format: logName.hash.timestamp
   */
  private long extractTimestampFromFilename(String filename) {
    if (filename == null) {
      return 0;
    }

    String[] parts = filename.split("\\.");
    if (parts.length >= 3) {
      try {
        // Last part should be timestamp
        return Long.parseLong(parts[parts.length - 1]);
      } catch (NumberFormatException e) {
        LOG.warn("Failed to parse timestamp from filename: {}", filename);
        return 0;
      }
    }

    return 0;
  }

  /**
   * Generates a map of default token values that can be used in the key format.
   *
   * @return a map of default token values
   */
  private Map<String, String> getDefaultTokenValue() {
    String timestamp = FORMATTER.format(new Date());
    Map<String, String> defaultTokenValues = new HashMap<>();
    for (DefaultTokens token : DefaultTokens.values()) {
      String value;
      switch (token) {
        case UUID:
          value = UUID.randomUUID().toString().substring(0, 8);
          break;
        case HOST:
          value = HOSTNAME;
          break;
        case LOGNAME:
          value = logName;
          break;
        case TIMESTAMP:
          value = timestamp;
          break;
        default:
          throw new IllegalStateException("Unexpected value: " + token);
      }
      defaultTokenValues.put(token.name(), value);
    }
    // Also allow for adding the timestamp in parts.
    defaultTokenValues.put("y", timestamp.substring(0,4));
    defaultTokenValues.put("M", timestamp.substring(4,6));
    defaultTokenValues.put("d", timestamp.substring(6,8));
    defaultTokenValues.put("H", timestamp.substring(8,10));
    defaultTokenValues.put("m", timestamp.substring(10,12));
    defaultTokenValues.put("S", timestamp.substring(12,14));
    return defaultTokenValues;
  }

  /**
   * Generates an S3 object key based on the configured key format. It uses {@link StringSubstitutor} to replace key tokens in the
   * s3KeyFormat that will be replaced with the values extracted from the log filename based on the regex pattern provided in
   * filenamePattern using named regex groups. We also support injection of environment variables and default tokens.
   *
   * Default tokens: {{TOKEN}}
   * Environment variables: ${ENV_VAR}
   * Named groups from filenamePattern: %{TOKEN}
   *
   * @return the generated S3 object key
   */
  public String generateS3ObjectKey() {
    String s3Key = keyFormat;
    String
        logFilenameOrAbsolutePath =
        matchAbsolutePath ? logStream.getFullPathPrefix() : logStream.getFileNamePrefix();
    Matcher matcher;
    // Replace default tokens in the "%TOKEN" format
    Map<String, String> defaultTokenValues = getDefaultTokenValue();
    StringSubstitutor stringSubstitutor = new StringSubstitutor(defaultTokenValues, "{{", "}}");
    s3Key = stringSubstitutor.replace(s3Key);

    // Replace environment variables
    if (envMappings != null || !envMappings.isEmpty()) {
      // Default replacement is with ${} format
      stringSubstitutor = new StringSubstitutor(envMappings);
      s3Key = stringSubstitutor.replace(s3Key);
    }

    // Replace named groups from filenamePattern
    if (filenameParsingEnabled) {
      if ((matcher = extractTokensFromFilename(logFilenameOrAbsolutePath)) != null) {
        Map<String, String> groupMap = new HashMap<>();
        for (String token : fileNameTokens) {
          // Attempt to replace the token in filenamePattern with the matched value
          String matchedValue = matcher.group(token);
          if (matchedValue != null) {
            groupMap.put(token, matchedValue);
          }
        }
        stringSubstitutor = new StringSubstitutor(groupMap, "%{", "}");
        s3Key = stringSubstitutor.replace(s3Key);
      } else {
        // If there is no match we simply return the key without replacing any custom tokens
        LOG.warn("Filename parsing is enabled but filenamePattern provided: " + filenamePattern
            + " does not match the log file: " + logFilenameOrAbsolutePath);
        OpenTsdbMetricConverter.incr(SingerMetrics.S3_WRITER + "no_filename_pattern_match", 1,
            "bucket=" + bucketName, "host=" + HOSTNAME, "logName=" + logName);
      }
    }
    LOG.info("Generated S3 object key: " + s3Key);
    return s3Key;
  }

  /**
   * Ends the commit process by flushing the buffer and handling the buffer file if it exceeds
   * the maximum file size.
   *
   * @param numLogMessagesRead the number of log messages read
   * @param isDraining whether the system is in a draining state
   * @throws LogStreamWriterException if an error occurs while ending the commit or if S3 upload fails when buffer is full
   */
  public synchronized void endCommit(int numLogMessagesRead, boolean isDraining)
      throws LogStreamWriterException {
    synchronized (objLock) {
      // Flush all buffered writes to disk
      try {
        bufferedOutputStream.flush();
      } catch (IOException e) {
        LOG.error("Failed to flush buffer file after batch write", e);
        throw new LogStreamWriterException("Failed to flush batch writes to disk", e);
      }

      long currentBufferSize = bufferFile.length();

      // Check if buffer size threshold is exceeded
      if (currentBufferSize >= maxFileSizeMB * BYTES_IN_MB) {
        LOG.info("Buffer file size {} has exceeded the size threshold of {}, attempting S3 upload",
            currentBufferSize, maxFileSizeMB * BYTES_IN_MB);
        uploadDiskBufferedFileToS3(TriggerType.SIZE);
      }
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
   * Closes the S3Writer by flushing any buffered data to disk and cancelling scheduled tasks.
   *
   * @throws IOException if there's an error closing the buffered output stream
   */
  @Override
  public void close() throws IOException {
    // Cancel periodic time-based upload task
    if (timeBasedUploadTask != null && !timeBasedUploadTask.isCancelled()) {
      timeBasedUploadTask.cancel(false);
      LOG.debug("Cancelled time-based upload task for log {}", logName);
    }

    // Close buffered output stream to ensure data is flushed to disk
    if (bufferedOutputStream != null) {
      bufferedOutputStream.close();
      bufferedOutputStream = null;
    }
  }
}