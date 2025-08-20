/**
 * Copyright 2019 Pinterest, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.singer.kubernetes;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.monitor.FileSystemEvent;
import com.pinterest.singer.monitor.FileSystemEventFetcher;
import com.pinterest.singer.thrift.configuration.KubeConfig;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.utils.SingerUtils;
import com.twitter.ostrich.stats.Stats;

/**
 * Service that detects new Kubernetes pods by watching directory and kubelet
 * metadata and creates events for any subscribing listeners.
 *
 * This class is a singleton.
 *
 * Note: this service starts and maintains 3 threads: Thread 1 - for kubernetes
 * md poll Thread 2 - for filesystemeventfetcher Thread 3 - for processing
 * filesystemeventfetcher events
 */
public class KubeService implements Runnable {

    private static final int MILLISECONDS_IN_SECONDS = 1000;
    private static final int MD_MAX_POLL_DELAY = 3600;
    private static final Logger LOG = LoggerFactory.getLogger(KubeService.class);
    private static KubeService instance;
    private static String kubeMdUrl;
    private static boolean useSecureConnection;
    private static SSLContext sslContext;
    private static Map<String, String> serviceAccountTokenHeaderMap;
    // using CSLS to avoid having to explicitly lock and simplify for updates
    // happening from both MD service and FileSystemEventFetcher since this may lead
    // to a deadlock since the update methods are shared
    private Set<String> activePodSet = new ConcurrentSkipListSet<>();
    private int pollFrequency;
    private Set<PodWatcher> registeredWatchers = new HashSet<>();
    private String podLogDirectory;
    private String ignorePodDirectory;
    private Thread thKubeServiceThread;
    private FileSystemEventFetcher fsEventFetcher;
    private Thread thFsEventThread;
    private int kubePollDelay;

    private KubeService() {
        SingerConfig config = SingerSettings.getSingerConfig();
        Preconditions.checkNotNull(config);
        KubeConfig kubeConfig = config.getKubeConfig();
        Preconditions.checkNotNull(kubeConfig);
        try {
            init(kubeConfig);
            // Register here so it is the first watcher in the set
            addWatcher(PodMetadataWatcher.getInstance());
        } catch (Exception e) {
            LOG.error("Exception while initializing KubeService", e);
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    protected KubeService(KubeConfig kubeConfig) {
        init(kubeConfig);
    }

    private void init(KubeConfig kubeConfig) {
        if (kubeConfig.getPollFrequencyInSeconds() > MD_MAX_POLL_DELAY) {
            throw new IllegalArgumentException("Too long kubeMdPollFrequency, should be configured to less than "
                    + MD_MAX_POLL_DELAY + " seconds");
        }
        // convert to milliseconds
        pollFrequency = kubeConfig.getPollFrequencyInSeconds() * MILLISECONDS_IN_SECONDS;
        podLogDirectory = kubeConfig.getPodLogDirectory();
        kubePollDelay = kubeConfig.getKubePollStartDelaySeconds() * MILLISECONDS_IN_SECONDS;
        ignorePodDirectory = kubeConfig.getIgnorePodDirectory();
        useSecureConnection = kubeConfig.isUseSecureConnection();
        if (useSecureConnection) {
            try {
                sslContext = getSslContext(kubeConfig.getServiceAccountCaCertPath());
            } catch (CertificateException | UnrecoverableKeyException | KeyStoreException |
                     IOException | NoSuchAlgorithmException | KeyManagementException e) {
                LOG.error("Error initializing SSLContext for KubeService", e);
                OpenTsdbMetricConverter.incr(SingerMetrics.KUBE_SERVICE_SECURE_CONNECTION_ERROR, "reason=ssl_init_error");
            }
            try {
                serviceAccountTokenHeaderMap = Collections.singletonMap("Authorization", "Bearer " + new String(
                    Files.readAllBytes(Paths.get(kubeConfig.getServiceAccountTokenPath()))));
            } catch (IOException e) {
                LOG.error("Error reading service account token for KubeService", e);
                OpenTsdbMetricConverter.incr(SingerMetrics.KUBE_SERVICE_SECURE_CONNECTION_ERROR, "reason=token_read_error");
            }
        }
        kubeMdUrl = getKubeMdUrl(kubeConfig.getKubeletPort());
    }

    public synchronized static KubeService getInstance() {
        if (instance == null) {
            instance = new KubeService();
        }
        return instance;
    }

    @Override
    public void run() {
      try {
        // fetch existing pod directories
        updatePodNamesFromFileSystem();
        // we should wait for some time
        Thread.sleep(kubePollDelay);
      } catch (InterruptedException e1) {
        LOG.error("Kube Service interrupted while waiting for poll delay to finish");
      } catch (Exception e) {
        LOG.error("Error while updating pod names from file system", e);
        Stats.incr(SingerMetrics.KUBE_SERVICE_ERROR);
      }

        while (true) {
            try {
                // method refactored so that class level method locking is done with the
                // synchronized keyword
                updatePodNames();
                LOG.debug("Pod IDs refreshed at:" + new Date(System.currentTimeMillis()));
            } catch (Exception e) {
                // TODO what should be done if we have errors in reaching MD service
                Stats.incr(SingerMetrics.KUBE_API_ERROR);
                LOG.error("Kubernetes service poll failed:" + e.getMessage());
            }

            try {
                Thread.sleep(pollFrequency);
            } catch (InterruptedException e) {
                LOG.warn("KubeService was interrupted, exiting loop");
                break;
            }
        }
    }

    /**
     * Load POD that are already on the file system
     * 
     * This method should have an effect on data if Singer was restarted
     */
    private void updatePodNamesFromFileSystem() {
        LOG.info("Kubernetes POD log directory configured as:" + podLogDirectory);
        File[] directories = new File(podLogDirectory).listFiles(new FileFilter() {

            @Override
            public boolean accept(File pathname) {
                return pathname.isDirectory();
            }
        });

        Set<String> temp = new HashSet<>();
        File[] files = new File(podLogDirectory).listFiles(new FileFilter() {

            @Override
            public boolean accept(File pathname) {
                return pathname.isFile();
            }
        });
        if (files != null) {
            for (File file : files) {
                LOG.debug("Found . file " + file.getName() + " in POD log directory " + podLogDirectory);
                temp.add(file.getName());
            }
        }

        if (directories != null) {
            LOG.info(
              "Found " + directories.length + " directories in POD log directory " + podLogDirectory);
            for (File directory : directories) {
                String podName = directory.getName();
                if (temp.contains("." + podName) || checkIgnoreDirectory(podName)) {
                    LOG.info("Ignoring POD directory " + podName
                        + " since there is a tombstone file present or has ignored directory inside");
                    // Skip adding this pod to the active pod set
                    continue;
                }

                activePodSet.add(podName);
                for (PodWatcher podWatcher : registeredWatchers) {
                    podWatcher.podCreated(podName);
                }
                LOG.info("Active POD found (via directory):" + podName);
            }
        }
        // update number of active pods running
        Stats.setGauge(SingerMetrics.NUMBER_OF_PODS, activePodSet.size());
    }

    /**
     * Clear the set of Pod Names and update it with the latest fetch from kubelet
     * 
     * Following a listener design, currently we only have 1 listener but in future
     * if we want to do something else as well when these events happen then this
     * might come in handy.
     * 
     * @throws IOException
     */
    public void updatePodNames() throws IOException {
        LOG.debug("Active podset:" + activePodSet);
        Set<String> updatedPodNames = fetchPodNamesFromMetadata();
        SetView<String> deletedNames = Sets.difference(activePodSet, updatedPodNames);

        // ignore new pods, pod discovery is done by watching directories

        for (String podName : deletedNames) {
            updatePodWatchers(podName, true);
            // update metrics since pods have been deleted
            Stats.incr(SingerMetrics.PODS_DELETED);
            Stats.incr(SingerMetrics.NUMBER_OF_PODS, -1);
        }
        LOG.debug("Fired events for registered watchers");

        activePodSet.clear();
        activePodSet.addAll(updatedPodNames);
        LOG.debug("Cleared and updated pod names:" + activePodSet);
    }

    /**
     * Update all {@link PodWatcher} about the pod that changed (created or deleted)
     * 
     * @param podName
     * @param isDelete
     */
    public void updatePodWatchers(String podName, boolean isDelete) {
        LOG.debug("Pod change:" + podName + " deleted:" + isDelete);
        for (PodWatcher watcher : registeredWatchers) {
            try {
                if (isDelete) {
                    watcher.podDeleted(podName);
                } else {
                    watcher.podCreated(podName);
                }
            } catch (Exception e) {
                LOG.error("Watcher had an exception", e);
            }
        }
    }

    /**
     * Fetch Pod IDs from metadata.
     * 
     * Note: inside singer we refer to pod's identifier as a the PodName
     * 
     * e.g. see src/test/resources/pods-goodresponse.json
     *
     * @return set of pod names
     * @throws IOException
     */
    public Set<String> fetchPodNamesFromMetadata() throws IOException {
        JsonArray podList = getPodListFromKubelet();
        Set<String> podNames = new HashSet<>();
        if (podList != null) {
            for (int i = 0; i < podList.size(); i++) {
                JsonObject metadata = podList.get(i).getAsJsonObject().get("metadata").getAsJsonObject();
                // pod name
                String name = metadata.get("name").getAsString();
                // to support namespace based POD directories
                // pod namespace
                String namespace = metadata.get("namespace").getAsString();
                // pod uid
                String podUid = metadata.get("uid").getAsString();

                // coexist of 2 format: namespace_podname or namespace_podname_uid
                String podDirectoryName = getPodDirectoryName(podLogDirectory, namespace, name, podUid);
                // Ignore pod if Ignore directory exists, this indicates that the pod is running its own dedicated logging agent (dual mode)
                if (checkIgnoreDirectory(podDirectoryName)) {
                  LOG.debug("Ignoring pod " + podDirectoryName + ", ignore flag found inside pod log directory");
                  OpenTsdbMetricConverter.incr(SingerMetrics.PODS_IGNORED, "podname=" + name);
                  continue;
                }
                podNames.add(podDirectoryName);
                LOG.debug("Found active POD name in JSON:" + name);
            }
        }
        LOG.debug("Pod names from kubelet:" + podNames);
        return podNames;
    }

    /**
     * Fetch pod list from kubelet
     *
     * @return
     * @throws IOException
     */
    public static JsonArray getPodListFromKubelet() throws IOException {
      String response = SingerUtils.makeGetRequest(kubeMdUrl, serviceAccountTokenHeaderMap, sslContext);
      Gson gson = new Gson();
      JsonObject obj = gson.fromJson(response, JsonObject.class);
      return obj != null && obj.has("items") ? obj.get("items").getAsJsonArray() : null;
    }


    /**
     * Return the poll frequency in milliseconds
     * 
     * @return poll frequency in milliseconds
     */
    public int getPollFrequency() {
        return pollFrequency;
    }

    /**
     * Get {@link Set} of active pods polled from kubelets.
     * 
     * Note: This is a point in time snapshot of the actual {@link Set} object (for
     * concurrency control)
     * 
     * @return
     */
    public Set<String> getActivePodSet() {
        // copy to a new hashset for MVCC design; prevents external classes from
        // modifying state
        return new HashSet<>(activePodSet);
    }

    /**
     * Add a watcher to the registered watcher set
     * 
     * @param watcher
     */
    public synchronized void addWatcher(PodWatcher watcher) {
        registeredWatchers.add(watcher);
    }

    /**
     * Remove a watcher from registered watcher set
     * 
     * @param watcher
     */
    public synchronized void removeWatcher(PodWatcher watcher) {
        registeredWatchers.remove(watcher);
    }

    /**
     * Starts the poll service.
     * 
     * Note: This method is idempotent
     */
    public void start() {
        if (thKubeServiceThread == null) {
            thKubeServiceThread = new Thread(this, "KubeService");
            thKubeServiceThread.setDaemon(true);
            thKubeServiceThread.start();

            try {
                fsEventFetcher = new FileSystemEventFetcher(SingerSettings.getSingerConfig());
                fsEventFetcher.start("KubernetesDirectory");
                LOG.info("Creating a file system monitor:" + podLogDirectory);
                fsEventFetcher.registerPath(new File(podLogDirectory).toPath());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            thFsEventThread = new Thread() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            checkAndProcessFsEvents();
                        } catch (InterruptedException e) {
                            LOG.warn("Kube fs event processor interrupted");
                            break;
                        }
                    }
                }
            };
            thFsEventThread.start();
        }

        LOG.info("Kube Service started");
    }

    /**
     * Stop kubernetes service
     */
    public void stop() {
        if (thKubeServiceThread != null) {
            thKubeServiceThread.interrupt();
            fsEventFetcher.stop();
            thFsEventThread.interrupt();
            activePodSet.clear();
            LOG.info("KubeService stopped");
            thKubeServiceThread = null;
        }
    }

    /**
     * Check if there are any new events available in the eventfetcher queue
     * 
     * @throws InterruptedException
     */
    public void checkAndProcessFsEvents() throws InterruptedException {
        // process events from fsEventFetcher
        FileSystemEvent event = fsEventFetcher.getEvent();
        WatchEvent.Kind<?> kind = event.event().kind();
        Path file = (Path) event.event().context();
        // should be NO use case for FS Modify
        // ignore delete events
        if (kind.equals(StandardWatchEventKinds.ENTRY_CREATE)) {
            if (!file.toFile().isFile()) {
                String podName = file.toFile().getName();
                boolean ignoreDir = checkIgnoreDirectory(podName);
                if (podName.startsWith(".") || ignoreDir) {
                    // ignore tombstone files & pod directories running a dedicated singer instance
                    if (ignoreDir) OpenTsdbMetricConverter.incr(SingerMetrics.PODS_IGNORED, "podname=" + ignoreDir);
                    return;
                }
                LOG.info("New pod directory discovered by FSM:" + event.logDir() + " " + podLogDirectory 
                    + " podname:" + podName);
                Stats.incr(SingerMetrics.PODS_CREATED);
                Stats.incr(SingerMetrics.NUMBER_OF_PODS);
                activePodSet.add(podName);
                updatePodWatchers(podName, false);
            }
            // ignore all events that are not directory create events
        } else if (kind.equals(StandardWatchEventKinds.OVERFLOW)) {
            LOG.warn("Received overflow watch event from filesystem: Events may have been lost");
            // perform a full sync on pod names from file system
            updatePodNamesFromFileSystem();
        } else if (kind.equals(StandardWatchEventKinds.ENTRY_DELETE)) {
            // ignore the . files
            if (!file.toFile().getName().startsWith(".")) {
                LOG.info("File deleted:" + file.toFile().getName());
            }
        }
    }

    /**
     * Used for unit testing to cleanup this singleton's state
     */
    public static void reset() {
        if (instance != null) {
            instance.stop();
            instance = null;
        }
    }

    private boolean checkIgnoreDirectory(String podName) {
        if (ignorePodDirectory == null || ignorePodDirectory.isEmpty()) return false;
        return Files.exists(Paths.get(podLogDirectory, podName, ignorePodDirectory));
    }

    public static String getPodDirectoryName(String dir, String namespace, String podName, String uuid) {
      String podFormat = namespace + "_" + podName;
      if (Files.exists(Paths.get(dir, podFormat))) {
          return podFormat;
      }
      podFormat = namespace + "_" + podName + "_" + uuid;
      return podFormat;
    }

    public String getKubeMdUrl(String port) {
        // In newer Kubernetes versions, the kubelet API is served over HTTPS. In addition, the certificate
        // has the node's IP address in its SANs, so we can't use localhost.
        if (useSecureConnection) {
            String hostIp = System.getenv("HOST_IP");
            if (hostIp == null || hostIp.isEmpty()) {
                LOG.warn("HOST_IP not set, using localhost for kubelet API secure access");
                return "https://localhost:" + port + "/pods";
            }
            LOG.info("Using HOST_IP for kubelet API: " + hostIp);
            return "https://" + hostIp + ":" + port + "/pods";
        }
        return "http://localhost:" + port + "/pods";
    }

    private static SSLContext getSslContext(String certPath)
        throws CertificateException, UnrecoverableKeyException, KeyStoreException, IOException, NoSuchAlgorithmException, KeyManagementException {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        SSLContext sslContext = SSLContext.getInstance("TLS");
        ByteArrayInputStream caInput = new ByteArrayInputStream(
            Files.readAllBytes(Paths.get(certPath)));
        X509Certificate cert = (X509Certificate) cf.generateCertificate(caInput);

        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null);
        ks.setCertificateEntry(cert.getSubjectX500Principal().getName(), cert);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, null);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(
            TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        sslContext.init(null, tmf.getTrustManagers(), null);
        return sslContext;
    }
}
