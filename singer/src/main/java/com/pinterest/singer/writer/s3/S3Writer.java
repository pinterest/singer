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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import java.util.List;
import java.util.Date;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.text.SimpleDateFormat;

import software.amazon.awssdk.services.s3.S3Client;

import static com.pinterest.singer.utils.SingerUtils.getHostnamePrefixes;

/**
 * A LogStreamWriter for Singer that writes to S3 (writer.type=s3).
 * */
public class S3Writer implements LogStreamWriter {
    public static final String HOSTNAME = SingerUtils.HOSTNAME;
    private static final Logger LOG = LoggerFactory.getLogger(S3Writer.class);
    private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    protected final LogStream logStream;
    private final String BUFFER_DIR;
    private static final int BYTES_IN_MB = 1024 * 1024;
    private static final int MIN_UPLOAD_TIME_IN_SECONDS = 30;
    private static final int MAX_FILE_SIZE_IN_MB = 50;

    private ObjectUploaderTask putObjectUploader;
    private S3Client s3Client;
    private final S3WriterConfig s3WriterConfig;

    // S3 information
    private String bucketName;
    private String keyPrefix;

    // Disk-buffered file that will eventually be uploaded to S3 if size or time thresholds are met
    private BufferedOutputStream bufferedOutputStream;
    private File bufferFile;

    // Custom Thresholds
    private int maxFileSizeMB;
    private int minUploadTime;
    private int maxRetries;

    // Timer for scheduling uploads
    private static ScheduledExecutorService fileUploadTimer;
    private Future<?> uploadFuture;
    private final Object objLock = new Object(); // used for synchronization locking

    private int messageCount = 0;

    static {
        ScheduledThreadPoolExecutor tmpTimer = new ScheduledThreadPoolExecutor(1);
        tmpTimer.setRemoveOnCancelPolicy(true);
        fileUploadTimer = tmpTimer;
    }

    /**
     * Constructs an S3Writer instance.
     *
     * @param logStream the LogStream associated with this writer
     * @param s3WriterConfig the S3WriterConfig containing configuration settings
     */
    public S3Writer(LogStream logStream, S3WriterConfig s3WriterConfig) {
        this.logStream = logStream;
        this.s3WriterConfig = s3WriterConfig;
        this.BUFFER_DIR = s3WriterConfig.getBufferDir();
        initialize();
    }

    // Static factory method for testing
    @VisibleForTesting
    public S3Writer(LogStream logStream, S3WriterConfig s3WriterConfig, S3Client s3Client, ObjectUploaderTask putObjectUploader, String path) {
        this.BUFFER_DIR = path;
        this.logStream = logStream;
        this.s3WriterConfig = s3WriterConfig;
        this.putObjectUploader = putObjectUploader;
        this.s3Client = s3Client;
        initialize();

    }

    private void initialize() {
        this.maxFileSizeMB = s3WriterConfig.getMaxFileSizeMB();
        this.minUploadTime = s3WriterConfig.getMinUploadTimeInSeconds();
        this.maxRetries = s3WriterConfig.getMaxRetries();

        // Create directory if it does not exist
        new File(BUFFER_DIR).mkdirs();
        String bufferFileName = sanitizeFileName(logStream.getFullPathPrefix()) + ".buffer.log";
        bufferFile = new File(BUFFER_DIR, bufferFileName);
        try {
            bufferFile.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.keyPrefix = s3WriterConfig.getKeyPrefix();

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
                putObjectUploader = new ObjectUploaderTask(s3Client, bucketName, keyPrefix, maxRetries);
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
     * Starts the commit process, initializing the buffer file and scheduling an upload task if not already scheduled.
     *
     * @param isDraining whether the system is in a draining state
     * @throws LogStreamWriterException if an error occurs while creating or writing to the buffer file
     */
    @Override
    public synchronized void startCommit(boolean isDraining) throws LogStreamWriterException {
        messageCount = 0;
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
     * The renamed file is then uploaded to S3 (or a similar storage service).
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
                            File newFile = createNewS3File();
                            String fileFormat = getS3FileFormat();
                            bufferedOutputStream.close();
                            uploadDiskBufferedFileToS3(newFile, fileFormat);
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
    private void uploadDiskBufferedFileToS3(File newFile, String fileFormat) throws IOException {
        if (bufferFile.renameTo(newFile)) {
            String bufferFileName = sanitizeFileName(logStream.getFullPathPrefix()) + ".buffer.log";
            bufferFile = new File(BUFFER_DIR, bufferFileName);
            bufferFile.createNewFile();
            bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(bufferFile, true));
            uploadAndRecordMetrics(newFile, fileFormat);
            newFile.delete();
        } else {
            LOG.error("Failed to rename buffer file");
        }
    }


    /**
     * Helper function that uploads the file to s3 and records metrics
     * */
    private void uploadAndRecordMetrics(File newFile, String fileFormat) {
        if (this.putObjectUploader.upload(newFile, fileFormat)) {
            OpenTsdbMetricConverter.incr(SingerMetrics.S3_WRITER + "num_uploads", 1,
                    "bucket=" + bucketName, "keyPrefix=" + keyPrefix, "host=" + HOSTNAME, "logName=" + logStream.getLogStreamName());
        } else  {
            OpenTsdbMetricConverter.incr(SingerMetrics.S3_WRITER + "num_failed_uploads", 1,
                    "bucket=" + bucketName, "keyPrefix=" + keyPrefix, "host=" + HOSTNAME, "logName=" + logStream.getLogStreamName());
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
                                                     boolean isDraining) throws LogStreamWriterException {
        try {
            writeMessageToBuffer(logMessageAndPosition);
            messageCount++;
        } catch (IOException e) {
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
    private void writeMessageToBuffer(LogMessageAndPosition logMessageAndPosition) throws IOException {
        byte[] logMessageBytes = logMessageAndPosition.logMessage.getMessage();
        bufferedOutputStream.write(logMessageBytes);
        bufferedOutputStream.write('\n'); // Add a newline character after each log message
        bufferedOutputStream.flush();
    }

    /**
     * Helper function to get the remaining part of the host name after the cluster prefix, typically a UUID.
     * */
    public static String extractHostSuffix(String inputStr) {
        String[] parts = inputStr.split("-");
        return parts[parts.length - 1];
    }

    /**
     * Helper function to get the file name without the .log extension.
     * */
    public static String getFileName(String input) {
        if (input.endsWith(".log")) {
            return input.substring(0, input.length() - 4);
        }
        return input;
    }

    /**
     * Gets the actual file name format for the S3 file.
     *
     * FORMAT: <log_name>/<service_fleet>/<host>/<log_dir>/<custom_filename>.<timestamp>
     * */
    public String getS3FileFormat() {

        String logName = logStream.getSingerLog().getSingerLogConfig().getName();
        List<String> hostPrefixes = getHostnamePrefixes("-");
        String serviceFleet = hostPrefixes.size() == 1 ? hostPrefixes.get(0) : hostPrefixes.get(hostPrefixes.size() - 2);
        String host = extractHostSuffix(HOSTNAME);
        String logDir = logStream.getFullPathPrefix().substring(1);
        String customFilename = s3WriterConfig.getFileNameFormat();
        String timestamp = FORMATTER.format(new Date());
        String returnedS3FileFormat = logName + "/" + serviceFleet + "/" + host + "/" + logDir + "/" + customFilename + "." + timestamp;
        LOG.info("Uploading the file: " + returnedS3FileFormat + " to the bucket " + bucketName + " with key prefix " + keyPrefix);
        return returnedS3FileFormat;
    }

    /**
     * Helper function to create a new file to upload to S3.
     * */
    private File createNewS3File() {
        String newFileName = s3WriterConfig.getFileNameFormat() + "." + FORMATTER.format(new Date());
        return new File(BUFFER_DIR, newFileName);
    }

    /**
     * Ends the commit process by flushing the buffer and handling the buffer file if it exceeds the maximum file size.
     *
     * @param numLogMessagesRead the number of log messages read
     * @param isDraining whether the system is in a draining state
     * @throws LogStreamWriterException if an error occurs while ending the commit
     */
    public synchronized void endCommit(int numLogMessagesRead, boolean isDraining) throws LogStreamWriterException {
        try {
            synchronized (objLock) {
                if (bufferFile.length() >= maxFileSizeMB * BYTES_IN_MB) {
                    if (uploadFuture != null) {
                        uploadFuture.cancel(true);
                    }
                    File newFile = createNewS3File();
                    String fileFormat = getS3FileFormat();
                    bufferedOutputStream.close();
                    uploadDiskBufferedFileToS3(newFile, fileFormat);
                    scheduleUploadTask();
                }
            }
        } catch (IOException e) {
            throw new LogStreamWriterException("Failed to end commit", e);
        }
    }

    /**
     * This method should not be used as it is Deprecated. Use comittable write method instead.
     * @param messages The LogMessages to be written.
     * @throws LogStreamWriterException
     */
    @Deprecated
    public void writeLogMessages(List<LogMessage> messages) throws LogStreamWriterException {
        throw new LogStreamWriterException("writeLogMessages is not supported. Use writeLogMessagesToCommit instead.");
    }


    /**
     * Closes the S3Writer, ensuring that remaining buffered log messages are safely uploaded to S3.
     *
     * This method synchronizes on {@code objLock} to ensure thread safety while performing the following steps:
     * Increments the close counter metric in OpenTsdb.
     * If the buffer file has remaining data, it renames the file for upload, closes the buffer stream,
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
                    "bucket=" + bucketName, "keyPrefix=" + keyPrefix, "host=" + HOSTNAME, "logName=" + logStream.getLogStreamName());

            if (bufferFile.length() > 0) {
                File newFile = createNewS3File();
                String fileFormat = getS3FileFormat();
                try {
                    bufferedOutputStream.close();
                    if (bufferFile.renameTo(newFile)) {
                        uploadAndRecordMetrics(newFile, fileFormat);
                        newFile.delete();
                    } else {
                        LOG.error("Failed to rename buffer file: " + bufferFile.getName());
                    }
                } catch (IOException e) {
                    LOG.error("Failed to close bufferedWriter or upload buffer file: " + bufferFile.getName(), e);
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