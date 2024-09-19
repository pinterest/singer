package com.pinterest.singer.writer.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.File;

public class ObjectUploaderTask {
    private static final Logger LOG = LoggerFactory.getLogger(S3Writer.class);
    private final S3Client s3Client;
    private final String bucket;
    private final String keyPrefix;
    private final int maxRetries;
    private static final long INITIAL_BACKOFF = 1000; // Initial backoff in milliseconds
    private static final long MAX_BACKOFF = 32000; // Maximum backoff in milliseconds

    public ObjectUploaderTask(S3Client s3Client, String bucket, String keyPrefix, int maxRetries) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.keyPrefix = keyPrefix;
        this.maxRetries = maxRetries;
    }

    /**
     * Uploads a file to S3 using the PutObject API.
     * Uses exponential backoff with a cap for retries.
     *
     * @param file is the actual file in disk to be uploaded
     * @param fileFormat is the key suffix
     * @return true if the file was successfully uploaded, false otherwise
     */
    public boolean upload(File file, String fileFormat) {
        int attempts = 0;
        boolean success = false;
        long backoff = INITIAL_BACKOFF;

        while (attempts < maxRetries && !success) {
            attempts++;
            try {
                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(keyPrefix + fileFormat)
                        .build();

                PutObjectResponse putObjectResponse = s3Client.putObject(putObjectRequest, file.toPath());

                if (putObjectResponse.sdkHttpResponse().isSuccessful()) {
                    LOG.info("Successfully uploaded file: {} on attempt {}", fileFormat, attempts);
                    success = true;
                } else {
                    LOG.error("Failed to upload file: {} on attempt {}", fileFormat, attempts);
                }
            } catch (Exception e) {
                LOG.error("Failed to upload file: {} on attempt {}. Error: {}", fileFormat, attempts, e.getMessage());
            }

            if (!success && attempts < maxRetries) {
                try {
                    LOG.info("Failed to upload file: {} on attempt {}. Retrying in {} ms...", fileFormat, attempts, backoff);
                    Thread.sleep(backoff);
                    backoff = Math.min(backoff * 2, MAX_BACKOFF); // Exponential backoff with a cap
                } catch (InterruptedException ie) {
                    LOG.error("Interrupted while waiting to retry file upload: {}", fileFormat, ie);
                }
            }
        }

        if (!success) {
            // TODO: this means data loss as Singer gives up uploading the file, which is not ideal. We need a fallback mechanism.
            LOG.error("Exhausted all attempts ({}) to upload file: {}", maxRetries, fileFormat);
            return false;
        }
        return true;
    }
}