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

/**
 * Service for fetching and caching pod metadata from the kubelet API.
 * 
 * This class provides lazy loading of pod metadata - metadata is fetched
 * on-demand when first requested and cached for subsequent access.
 * 
 */
public class PodMetadataFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(PodMetadataFetcher.class);
  private final Map<String, Map<String,String>> podMetadata = new ConcurrentHashMap<>();
  private final List<String> podMetadataFields;
  private final String podLogDirectory;
  private static PodMetadataFetcher instance;


  public static PodMetadataFetcher getInstance() {
    if (instance == null) {
      synchronized (PodMetadataFetcher.class) {
        if (instance == null) {
          instance = new PodMetadataFetcher();
        }
      }
    }
    return instance;
  }

  @VisibleForTesting
  public static void reset() {
    instance = null;
  }

  protected PodMetadataFetcher() {
    SingerConfig config = SingerSettings.getSingerConfig();
    Preconditions.checkNotNull(config);
    KubeConfig kubeConfig = config.getKubeConfig();
    Preconditions.checkNotNull(kubeConfig);
    podLogDirectory = kubeConfig.getPodLogDirectory();
    if (kubeConfig.getPodMetadataFields() == null || kubeConfig.getPodMetadataFields().isEmpty()) {
      LOG.warn("Pod metadata fields are not set in the config. Pod metadata will not be fetched.");
    }
    podMetadataFields = kubeConfig.getPodMetadataFields();
  }

  @VisibleForTesting
  protected PodMetadataFetcher(KubeConfig kubeConfig) {
    Preconditions.checkNotNull(kubeConfig);
    podMetadataFields = kubeConfig.getPodMetadataFields();
    podLogDirectory = kubeConfig.getPodLogDirectory();
  }

  @VisibleForTesting
  public Map<String, Map<String,String>> getPodMetadataMap() {
    return new HashMap<>(podMetadata);
  }

  /**
   * Get metadata for a pod, fetching from kubelet if not already cached.
   * 
   * @param podUid the pod directory name (namespace_name_uid format)
   * @return the pod metadata map, or null if unavailable
   */
  public Map<String, String> getPodMetadata(String podUid) {
    // Return cached if available
    if (podMetadata.containsKey(podUid)) {
      return podMetadata.get(podUid);
    }
    
    // Fetch and cache
    return fetchAndCache(podUid);
  }

  /**
   * Remove a pod's metadata from the cache.
   * Should be called when a pod is deleted.
   * 
   * @param podUid the pod directory name
   */
  public void remove(String podUid) {
    podMetadata.remove(podUid);
    OpenTsdbMetricConverter.gauge(SingerMetrics.POD_METADATA_MAP_SIZE, podMetadata.size());
    LOG.debug("Removed pod metadata from cache: {}", podUid);
  }

  /**
   * Fetch metadata from kubelet and cache it.
   * Synchronized to prevent duplicate fetches for the same pod.
   */
  private synchronized Map<String, String> fetchAndCache(String podUid) {    
    if (podMetadataFields == null || podMetadataFields.isEmpty()) {
      return null;
    }
    
    try {
      JsonArray podList = KubeService.getPodListFromKubelet();
      if (podList != null) {
        for (int i = 0; i < podList.size(); i++) {
          JsonObject metadata = podList.get(i).getAsJsonObject().get("metadata").getAsJsonObject();
          String podDirectoryName = KubeService.getPodDirectoryName(
              podLogDirectory,
              metadata.get("namespace").getAsString(),
              metadata.get("name").getAsString(),
              metadata.get("uid").getAsString()
          );
          
          if (podDirectoryName.equals(podUid)) {
            Map<String, String> extracted = extractPodMetadataFields(metadata, podMetadataFields);
            podMetadata.put(podUid, extracted);
            LOG.info("Fetched and cached metadata for pod: {} is {}", podUid, extracted);
            OpenTsdbMetricConverter.gauge(SingerMetrics.POD_METADATA_MAP_SIZE, podMetadata.size());
            OpenTsdbMetricConverter.incr(SingerMetrics.POD_METADATA_UPDATED,
                "podName=" + podUid, "namespace=" + metadata.get("namespace").getAsString());
            return extracted;
          }
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to fetch metadata for pod: {}", podUid, e);
    }
    
    return null;
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
        extractedFields.putIfAbsent(fieldKey, currentElement.getAsString());
      }
    }
    return extractedFields;
  }
}
