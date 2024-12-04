package com.pinterest.singer.writer.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;

public class S3ClientManagerTest {

  @Test
  public void testCreateMultipleS3Clients() {
    S3ClientManager s3ClientManager = S3ClientManager.getInstance();
    S3Client s3Client1 = s3ClientManager.get("us-east-1");
    S3Client s3Client2 = s3ClientManager.get("us-west-2");
    S3Client s3Client3 = s3ClientManager.get("us-east-1");
    S3Client s3Client4 = s3ClientManager.get("us-west-2");
    assertEquals(s3Client1, s3Client3);
    assertEquals(s3Client2, s3Client4);
    assertNotEquals(s3Client1, s3Client2);
    assertEquals(2, s3ClientManager.getS3ClientMap().size());
  }
}
