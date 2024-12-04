package com.pinterest.singer.writer.s3;

import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Singleton class that holds a region -> S3Client mapping for S3 client
 * reuse and multi-region support.
 */
public class S3ClientManager {

  private static final Logger LOG = LoggerFactory.getLogger(S3ClientManager.class);
  private static S3ClientManager s3UploaderManagerInstance = null;

  static {
    S3ClientManager.getInstance();
  }

  public static S3ClientManager getInstance() {
    if (s3UploaderManagerInstance == null) {
      synchronized (S3ClientManager.class) {
        if (s3UploaderManagerInstance == null) {
          s3UploaderManagerInstance = new S3ClientManager();
        }
      }
    }
    return s3UploaderManagerInstance;
  }

  private ConcurrentHashMap<String, S3Client> s3ClientMap;

  private S3ClientManager() {
    s3ClientMap = new ConcurrentHashMap<>();
  }

  public static void shutdown() {
    S3ClientManager.getInstance().closeS3Clients();
  }

  public S3Client get(String region) {
    // For we only use the region as the key, in the future we can construct the key with more
    // fields (e.g endpoint + region).
    String key = region;
    // We don't check if the client is closed here because S3 clients are meant to be
    // long-lived objects, and they are not closed by the writers.
    if (s3ClientMap.containsKey(key)) {
      return s3ClientMap.get(key);
    }
    S3Client s3Client = S3Client.builder()
        .region(Region.of(region))
        .build();
    s3ClientMap.put(key, s3Client);
    OpenTsdbMetricConverter.addMetric(SingerMetrics.S3_WRITER + "num_clients", s3ClientMap.size());
    return s3Client;
  }

  private void closeS3Clients() {
    s3ClientMap.forEach((key, client) -> {
      try {
        client.close();
      } catch (Exception e) {
        LOG.error("Failed to close S3Client: {} ", key, e);
      }
    });
  }

  @VisibleForTesting
  public Map<String, S3Client> getS3ClientMap() {
    return s3ClientMap;
  }
}
