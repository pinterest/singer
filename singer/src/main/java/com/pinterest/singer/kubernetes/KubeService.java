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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

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

    private static final String DEFAULT_KUBELET_URL = "http://localhost:10255/pods";
    private static final String KUBE_MD_URL = "kube.md.url";
    private static final int MILLISECONDS_IN_SECONDS = 1000;
    private static final int MD_MAX_POLL_DELAY = 3600;
    private static final Logger LOG = LoggerFactory.getLogger(KubeService.class);
    private static final String PODS_MD_URL = System.getProperty(KUBE_MD_URL, DEFAULT_KUBELET_URL);
    private static KubeService instance;
    // using CSLS to avoid having to explicitly lock and simplify for updates
    // happening from both MD service and FileSystemEventFetcher since this may lead
    // to a deadlock since the update methods are shared
    private Set<String> activePodSet = new ConcurrentSkipListSet<>();
    private int pollFrequency;
    private Set<PodWatcher> registeredWatchers = new HashSet<>();
    private String podLogDirectory;
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
    }

    public synchronized static KubeService getInstance() {
        if (instance == null) {
            instance = new KubeService();
        }
        return instance;
    }

    @Override
    public void run() {
        // fetch existing pod directories
        updatePodNamesFromFileSystem();
        
        // we should wait for some time 
        try {
            Thread.sleep(kubePollDelay);
        } catch (InterruptedException e1) {
            LOG.error("Kube Service interrupted while waiting for poll delay to finish");
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
            for (File directory : directories) {
                String podName = directory.getName();
                if (temp.contains("." + podName)) {
                    LOG.info("Ignoring POD directory " + podName + " since there is a tombstone file present");
                    // Skip adding this pod to the active podset
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
        LOG.debug("Attempting to make pod md request");
        String response = SingerUtils.makeGetRequest(PODS_MD_URL);
        LOG.debug("Received pod md response:" + response);
        Gson gson = new Gson();
        JsonObject obj = gson.fromJson(response, JsonObject.class);
        LOG.debug("Finished parsing kubelet response for PODs; now extracting POD names");
        Set<String> podNames = new HashSet<>();
        if (obj != null && obj.has("items")) {
            JsonArray ary = obj.get("items").getAsJsonArray();
            for (int i = 0; i < ary.size(); i++) {
                JsonObject metadata = ary.get(i).getAsJsonObject().get("metadata").getAsJsonObject();
                String name = metadata.get("name").getAsString();
                podNames.add(name);
                // to support namespace based POD directories
                String namespace = metadata.get("namespace").getAsString();
                podNames.add(namespace + "_" + name);
                LOG.debug("Found active POD name in JSON:" + name);
            }
        }
        LOG.debug("Pod names from kubelet:" + podNames);
        return podNames;
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
                fsEventFetcher = new FileSystemEventFetcher();
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
                if (podName.startsWith(".")) {
                    // ignore tombstone files
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
}
