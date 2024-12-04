package com.pinterest.singer.writer.s3;

import com.pinterest.singer.thrift.configuration.S3WriterConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.File;

public class PutObjectUploader extends S3Uploader {
    private static final Logger LOG = LoggerFactory.getLogger(PutObjectUploader.class);
    private final int maxRetries;
    private static final long INITIAL_BACKOFF = 1000; // Initial backoff in milliseconds
    private static final long MAX_BACKOFF = 32000; // Maximum backoff in milliseconds

    public PutObjectUploader(S3WriterConfig s3WriterConfig, S3Client s3Client) {
        super(s3WriterConfig, s3Client);
        this.maxRetries = s3WriterConfig.getMaxRetries();
    }

    /**
     * Uploads a file to S3 using the PutObject API.
     * Uses exponential backoff with a cap for retries.
     *
     * @param s3ObjectUpload the object to upload
     * @return true if the file was successfully uploaded, false otherwise
     */
    @Override
    public boolean upload(S3ObjectUpload s3ObjectUpload) {
        int attempts = 0;
        boolean success = false;
        long backoff = INITIAL_BACKOFF;
        String s3Key = s3ObjectUpload.getKey();

        while (attempts < maxRetries && !success) {
            attempts++;
            try {
                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(s3Key)
                        .build();

                if (cannedAcl != null) {
                    putObjectRequest = putObjectRequest.toBuilder().acl(cannedAcl).build();
                }

                PutObjectResponse
                    putObjectResponse =
                    s3Client.putObject(putObjectRequest, s3ObjectUpload.getFile().toPath());

                if (putObjectResponse.sdkHttpResponse().isSuccessful()) {
                    LOG.info("Successfully uploaded file: {} on attempt {}", s3Key, attempts);
                    success = true;
                } else {
                    LOG.error("Failed to upload file: {} on attempt {}", s3Key, attempts);
                }
            } catch (Exception e) {
                LOG.error("Failed to upload file: {} on attempt {}. Error: {}", s3Key, attempts, e.getMessage());
            }

            if (!success && attempts < maxRetries) {
                try {
                    LOG.info("Failed to upload file: {} on attempt {}. Retrying in {} ms...", s3Key, attempts, backoff);
                    Thread.sleep(backoff);
                    backoff = Math.min(backoff * 2, MAX_BACKOFF); // Exponential backoff with a cap
                } catch (InterruptedException ie) {
                    LOG.error("Interrupted while waiting to retry file upload: {}", s3Key, ie);
                }
            }
        }

        if (!success) {
            // TODO: this means data loss as Singer gives up uploading the file, which is not ideal. We need a fallback mechanism.
            LOG.error("Exhausted all attempts ({}) to upload file: {}", maxRetries, s3Key);
            return false;
        }
        return true;
    }
}
