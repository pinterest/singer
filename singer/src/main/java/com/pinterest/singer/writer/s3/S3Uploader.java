package com.pinterest.singer.writer.s3;

import com.pinterest.singer.thrift.configuration.S3WriterConfig;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

/**
 * Abstract class for uploading S3 objects.
 */
public abstract class S3Uploader {
  protected S3WriterConfig s3WriterConfig;
  protected final ObjectCannedACL cannedAcl;
  protected final String bucket;
  protected final String contentType;
  protected S3Client s3Client;

  public S3Uploader(S3WriterConfig s3WriterConfig, S3Client s3Client) {
    this.s3WriterConfig = s3WriterConfig;
    this.bucket = s3WriterConfig.getBucket();
    this.cannedAcl = ObjectCannedACL.fromValue(s3WriterConfig.getCannedAcl());
    this.contentType = s3WriterConfig.getContentType();
    this.s3Client = s3Client;
  }

  /**
   * Uploads the given S3ObjectUpload to S3.
   *
   * @param s3ObjectUpload The S3ObjectUpload to upload.
   * @return true if the upload was successful, false otherwise.
   */
  public abstract boolean upload(S3ObjectUpload s3ObjectUpload);
}
