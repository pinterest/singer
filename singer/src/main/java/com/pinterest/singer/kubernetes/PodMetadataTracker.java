package com.pinterest.singer.kubernetes;

import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.configuration.KubeConfig;
import com.pinterest.singer.thrift.configuration.SingerConfig;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PodMetadataTracker implements PodWatcher {
  private static final Logger LOG = LoggerFactory.getLogger(PodMetadataTracker.class);
  private final Map<String, Map<String,String>> podMetadata = new ConcurrentHashMap<>();
  private final List<String> podMetadataFields;
  private final String podLogDirectory;
  private static PodMetadataTracker instance;


  public static PodMetadataTracker getInstance() {
    if (instance == null) {
      synchronized (PodMetadataTracker.class) {
        if (instance == null) {
          instance = new PodMetadataTracker();
        }
      }
    }
    return instance;
  }

  protected PodMetadataTracker() {
    SingerConfig config = SingerSettings.getSingerConfig();
    Preconditions.checkNotNull(config);
    KubeConfig kubeConfig = config.getKubeConfig();
    Preconditions.checkNotNull(kubeConfig);
    podLogDirectory = kubeConfig.getPodLogDirectory();
    if (kubeConfig.getPodMetadataFields() == null || kubeConfig.getPodMetadataFields().isEmpty()) {
      LOG.warn("Pod metadata fields are not set in the config. Pod metadata will not be updated.");
    }
    podMetadataFields = kubeConfig.getPodMetadataFields();
  }

  @VisibleForTesting
  protected PodMetadataTracker(KubeConfig kubeConfig) {
    Preconditions.checkNotNull(kubeConfig);
    podMetadataFields = kubeConfig.getPodMetadataFields();
    podLogDirectory = kubeConfig.getPodLogDirectory();
  }

  @VisibleForTesting
  public Map<String, Map<String,String>> getPodMetadataMap() {
    return new HashMap<>(podMetadata);
  }

  @Override
  public void podCreated(String podUid) {
    try {
      if (!podMetadata.containsKey(podUid)) {
        updatePodMetadata(podUid);
        OpenTsdbMetricConverter.gauge(SingerMetrics.POD_METADATA_MAP_SIZE, podMetadata.size());
      }
    } catch (IOException e) {
      LOG.error("Failed to update pod metadata for pod: {}", podUid, e);
    }
  }

  @Override
  public void podDeleted(String podUid) {
    podMetadata.remove(podUid);
    OpenTsdbMetricConverter.gauge(SingerMetrics.POD_METADATA_MAP_SIZE, podMetadata.size());
  }

  /**
   * Update podMetadata from kubelet for active pods. This method is only called after
   * the initial file system pod discovery.
   *
   * @throws IOException
   */
  private void updatePodMetadata(String podUid) throws IOException {
    if (podMetadataFields == null || podMetadataFields.isEmpty()) {
      return;
    }
    JsonArray podList = KubeService.getPodListFromKubelet();
    if (podList != null) {
      for (int i = 0; i < podList.size(); i++) {
        JsonObject metadata = podList.get(i).getAsJsonObject().get("metadata").getAsJsonObject();
        String
            podDirectoryName =
            KubeService.getPodDirectoryName(podLogDirectory, metadata.get("namespace").getAsString(),
                metadata.get("name").getAsString(), metadata.get("uid").getAsString());
        if (podDirectoryName.equals(podUid)) {
          podMetadata.put(podUid, extractPodMetadataFields(metadata, podMetadataFields));
          LOG.info("Pod metadata updated for pod: {}", podUid);
          OpenTsdbMetricConverter.incr(SingerMetrics.POD_METADATA_UPDATED,
              "podName=" + podUid, "namespace=" + metadata.get("namespace").getAsString());
        }
      }
    }
  }

  /**
   * Extract select fields from pod metadata items. Each nested field should be separated by a colon, e.g:
   * "labels:app" will extract the "app" field from the "labels" object.
   *
   * @param metadata pod metadata
   * @param podMetadataFields list of fields to extract
   * @return a JsonObject containing the extracted fields
   */
  public Map<String, String> extractPodMetadataFields(JsonObject metadata, List<String> podMetadataFields) {
    Map<String, String> extractedFields = new HashMap<>();
    for (String fieldPath : podMetadataFields) {
      JsonElement currentElement = metadata;
      String fieldKey = null;
      for (String key : fieldPath.split(":")) {
        if (currentElement == null || !currentElement.isJsonObject()) {
          currentElement = null;
          break;
        }
        currentElement = currentElement.getAsJsonObject().get(key);
        fieldKey = key;
      }
      if (fieldKey != null && currentElement != null && currentElement.isJsonPrimitive()) {
        extractedFields.putIfAbsent(fieldKey, currentElement.toString());
      }
    }
    return extractedFields;
  }

  public Map<String, String> getPodMetadata(String podName) {
    return podMetadata.get(podName) != null ? podMetadata.get(podName) : null;
  }
}