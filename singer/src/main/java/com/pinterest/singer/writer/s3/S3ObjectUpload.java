package com.pinterest.singer.writer.s3;

import java.io.File;

/**
 * Represents an S3 object upload.
 */
public class S3ObjectUpload {

  private final String key;
  private final File file;

  public S3ObjectUpload(String key, File file) {
    this.key = key;
    this.file = file;
  }

  public String getKey() {
    return key;
  }

  public File getFile() {
    return file;
  }
}
