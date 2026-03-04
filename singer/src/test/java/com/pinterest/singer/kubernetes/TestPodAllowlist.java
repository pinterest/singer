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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.pinterest.singer.SingerTestBase;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.monitor.LogStreamManager;
import com.pinterest.singer.thrift.configuration.FileNameMatchMode;
import com.pinterest.singer.thrift.configuration.KubeConfig;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;

/**
 * Tests for the pod allowlist feature which filters log stream initialization
 * based on pod metadata (name).
 * 
 * Uses the shared HTTP server from TestKubeService and pods-goodresponse.json.
 */
public class TestPodAllowlist {

    private SingerConfig config;
    private KubeConfig kubeConfig;
    private String podLogPath;
    private Path tempDir;

    // Pod directory names from pods-goodresponse.json: namespace_name_uid
    private static final String POD_NGINX_1 = "default_nginx-deployment-5c689d7589-abcde_12345678-1234-1234-1234-1234567890ab";
    private static final String POD_BACKEND = "default_backend-service-7987d5b5c-12345_54321678-9876-5432-9876-5432198765ac";
    private static final String POD_DATABASE = "default_database-7f8d5b7c6-mnopq_98765432-7654-4321-6543-987654321098";

    @BeforeClass
    public static void beforeClass() throws IOException {
        TestKubeService.ensureServerRunning();
    }

    @AfterClass
    public static void afterClass() {
        TestKubeService.removePodsContext();
    }

    @Before
    public void before() throws IOException {
        TestKubeService.removePodsContext();
        TestKubeService.registerGoodResponse();

        LogStreamManager.getInstance().getSingerLogPaths().clear();
        SingerSettings.getFsMonitorMap().clear();
        LogStreamManager.reset();
        KubeService.reset();
        PodMetadataFetcher.reset();
        SingerSettings.reset();

        SingerSettings.setBackgroundTaskExecutor(
            Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).build()));

        config = new SingerConfig();
        config.setKubernetesEnabled(true);
        SingerSettings.setSingerConfig(config);

        kubeConfig = new KubeConfig();
        config.setKubeConfig(kubeConfig);

        tempDir = Files.createTempDirectory("pods_allowlist_test");
        podLogPath = tempDir.toAbsolutePath().toString();
        kubeConfig.setPodLogDirectory(podLogPath);
        kubeConfig.setPodMetadataFields(Arrays.asList("name"));
    }

    @After
    public void after() {
        TestKubeService.removePodsContext();
        SingerSettings.getFsMonitorMap().clear();
        if (tempDir != null) {
            deleteDirectory(tempDir.toFile());
        }
        LogStreamManager.reset();
        KubeService.reset();
        PodMetadataFetcher.reset();
        SingerSettings.reset();
    }

    @Test
    public void testAllowlistDisabledWhenMetadataKeyNotConfigured() throws Exception {
        // Don't set podAllowlistMetadataKey - feature should be disabled
        SingerLogConfig logConfig = createLogConfig("test-log", "/var/log", "app.log");
        logConfig.setPodAllowlist(Arrays.asList("nginx-deployment-5c689d7589-abcde")); // Only allow this pod

        config.setLogConfigs(Arrays.asList(logConfig));
        SingerSettings.getLogConfigMap().putAll(SingerSettings.loadLogConfigMap(config));

        createPodDirectory(POD_BACKEND, "/var/log", "app.log");

        LogStreamManager lsm = LogStreamManager.getInstance();
        KubeService instance = KubeService.getInstance();
        instance.start();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        assertEquals("Should initialize because allowlist feature is disabled", 
            1, lsm.getSingerLogPaths().size());

        instance.stop();
    }

    @Test
    public void testAllowlistMatchAllowsInitialization() throws Exception {
        kubeConfig.setPodAllowlistMetadataKey("name");

        SingerLogConfig logConfig = createLogConfig("test-log", "/var/log", "app.log");
        logConfig.setPodAllowlist(Arrays.asList(
            "nginx-deployment-5c689d7589-abcde", 
            "nginx-deployment-5c689d7589-fghij"));

        config.setLogConfigs(Arrays.asList(logConfig));
        SingerSettings.getLogConfigMap().putAll(SingerSettings.loadLogConfigMap(config));

        createPodDirectory(POD_NGINX_1, "/var/log", "app.log");

        LogStreamManager lsm = LogStreamManager.getInstance();
        KubeService instance = KubeService.getInstance();
        instance.start();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        assertEquals("Pod nginx-1 should be initialized", 1, lsm.getSingerLogPaths().size());
        assertTrue(lsm.getSingerLogPaths().containsKey(podLogPath + "/" + POD_NGINX_1 + "/var/log"));

        instance.stop();
    }

    @Test
    public void testAllowlistNoMatchSkipsInitialization() throws Exception {
        kubeConfig.setPodAllowlistMetadataKey("name");

        SingerLogConfig logConfig = createLogConfig("test-log", "/var/log", "app.log");
        // Only allow nginx pods
        logConfig.setPodAllowlist(Arrays.asList(
            "nginx-deployment-5c689d7589-abcde", 
            "nginx-deployment-5c689d7589-fghij"));

        config.setLogConfigs(Arrays.asList(logConfig));
        SingerSettings.getLogConfigMap().putAll(SingerSettings.loadLogConfigMap(config));

        createPodDirectory(POD_BACKEND, "/var/log", "app.log");

        LogStreamManager lsm = LogStreamManager.getInstance();
        KubeService instance = KubeService.getInstance();
        instance.start();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        assertEquals("Pod backend should be skipped", 0, lsm.getSingerLogPaths().size());

        instance.stop();
    }

    @Test
    public void testConfigWithoutAllowlistInitializesForAllPods() throws Exception {
        kubeConfig.setPodAllowlistMetadataKey("name");

        SingerLogConfig logConfig = createLogConfig("test-log", "/var/log", "app.log");

        config.setLogConfigs(Arrays.asList(logConfig));
        SingerSettings.getLogConfigMap().putAll(SingerSettings.loadLogConfigMap(config));

        createPodDirectory(POD_DATABASE, "/var/log", "app.log");

        LogStreamManager lsm = LogStreamManager.getInstance();
        KubeService instance = KubeService.getInstance();
        instance.start();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        assertEquals("Pod should be initialized - config has no allowlist", 1, lsm.getSingerLogPaths().size());

        instance.stop();
    }

    @Test
    public void testMultipleConfigsWithDifferentAllowlists() throws Exception {
        kubeConfig.setPodAllowlistMetadataKey("name");

        SingerLogConfig logConfig1 = createLogConfig("log-nginx", "/var/log/nginx", "nginx.log");
        logConfig1.setPodAllowlist(Arrays.asList(
            "nginx-deployment-5c689d7589-abcde",
            "nginx-deployment-5c689d7589-fghij"));

        SingerLogConfig logConfig2 = createLogConfig("log-backend", "/var/log/backend", "backend.log");
        logConfig2.setPodAllowlist(Arrays.asList("backend-service-7987d5b5c-12345"));

        SingerLogConfig logConfig3 = createLogConfig("log-universal", "/var/log/common", "common.log");

        config.setLogConfigs(Arrays.asList(logConfig1, logConfig2, logConfig3));
        SingerSettings.getLogConfigMap().putAll(SingerSettings.loadLogConfigMap(config));

        new File(podLogPath + "/" + POD_NGINX_1 + "/var/log/nginx").mkdirs();
        new File(podLogPath + "/" + POD_NGINX_1 + "/var/log/nginx/nginx.log").createNewFile();
        new File(podLogPath + "/" + POD_NGINX_1 + "/var/log/backend").mkdirs();
        new File(podLogPath + "/" + POD_NGINX_1 + "/var/log/backend/backend.log").createNewFile();
        new File(podLogPath + "/" + POD_NGINX_1 + "/var/log/common").mkdirs();
        new File(podLogPath + "/" + POD_NGINX_1 + "/var/log/common/common.log").createNewFile();

        LogStreamManager lsm = LogStreamManager.getInstance();
        KubeService instance = KubeService.getInstance();
        instance.start();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        assertEquals("Should have 2 log paths initialized", 2, lsm.getSingerLogPaths().size());
        assertTrue("Should have /var/log/nginx", 
            lsm.getSingerLogPaths().containsKey(podLogPath + "/" + POD_NGINX_1 + "/var/log/nginx"));
        assertTrue("Should have /var/log/common", 
            lsm.getSingerLogPaths().containsKey(podLogPath + "/" + POD_NGINX_1 + "/var/log/common"));
        assertFalse("Should NOT have /var/log/backend", 
            lsm.getSingerLogPaths().containsKey(podLogPath + "/" + POD_NGINX_1 + "/var/log/backend"));

        instance.stop();
    }

    @Test
    public void testHostOnlyConfigSkipsPodsInitialization() throws Exception {
        kubeConfig.setPodAllowlistMetadataKey("name");

        SingerLogConfig hostOnlyConfig = createLogConfig("host-only-log", "/var/log", "host.log");
        hostOnlyConfig.setPodAllowlist(Arrays.asList("__HOST__"));

        config.setLogConfigs(Arrays.asList(hostOnlyConfig));
        SingerSettings.getLogConfigMap().putAll(SingerSettings.loadLogConfigMap(config));

        createPodDirectory(POD_NGINX_1, "/var/log", "host.log");

        LogStreamManager lsm = LogStreamManager.getInstance();
        KubeService instance = KubeService.getInstance();
        instance.start();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        assertEquals("Host-only config should be skipped for pods", 0, lsm.getSingerLogPaths().size());

        instance.stop();
    }

    @Test
    public void testIncludeHostMarkerWithPodIds() throws Exception {
        kubeConfig.setPodAllowlistMetadataKey("name");

        Path hostLogDir = Files.createTempDirectory("host_logs_test");
        String hostLogPath = hostLogDir.toAbsolutePath().toString();
        new File(hostLogPath + "/mixed.log").createNewFile();

        SingerLogConfig mixedConfig = createLogConfig("mixed-log", hostLogPath, "mixed.log");
        mixedConfig.setPodAllowlist(Arrays.asList(
            "__HOST__",
            "nginx-deployment-5c689d7589-abcde"));

        config.setLogConfigs(Arrays.asList(mixedConfig));
        SingerSettings.getLogConfigMap().putAll(SingerSettings.loadLogConfigMap(config));

        LogStreamManager.initializeLogStreams();
        LogStreamManager lsm = LogStreamManager.getInstance();

        assertTrue("Host-level log should be initialized", 
            lsm.getSingerLogPaths().containsKey(hostLogPath));

        createPodDirectory(POD_NGINX_1, hostLogPath, "mixed.log");

        KubeService instance = KubeService.getInstance();
        instance.start();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        assertEquals("Should have 2 log paths - host and pod", 2, lsm.getSingerLogPaths().size());
        assertTrue("Host-level log should be initialized", 
            lsm.getSingerLogPaths().containsKey(hostLogPath));
        assertTrue("Pod-level log should be initialized", 
            lsm.getSingerLogPaths().containsKey(podLogPath + "/" + POD_NGINX_1 + hostLogPath));

        instance.stop();
        deleteDirectory(hostLogDir.toFile());
    }

    @Test
    public void testPrefixMatchingWithPodIds() throws Exception {
        kubeConfig.setPodAllowlistMetadataKey("name");

        SingerLogConfig prefixConfig = createLogConfig("prefix-log", "/var/log", "prefix.log");
        prefixConfig.setPodAllowlist(Arrays.asList("nginx-"));

        config.setLogConfigs(Arrays.asList(prefixConfig));
        SingerSettings.getLogConfigMap().putAll(SingerSettings.loadLogConfigMap(config));

        createPodDirectory(POD_NGINX_1, "/var/log", "prefix.log");

        LogStreamManager lsm = LogStreamManager.getInstance();
        KubeService instance = KubeService.getInstance();
        instance.start();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS * 3);

        assertEquals("Prefix matching should work - 'nginx-' matches 'nginx-deployment-...'", 
            1, lsm.getSingerLogPaths().size());

        instance.stop();
    }

    private SingerLogConfig createLogConfig(String name, String logDir, String regex) {
        SingerLogConfig logConfig = new SingerLogConfig();
        logConfig.setName(name);
        logConfig.setLogDir(logDir);
        logConfig.setLogStreamRegex(regex);
        logConfig.setFilenameMatchMode(FileNameMatchMode.PREFIX);
        return logConfig;
    }

    private void createPodDirectory(String podUid, String logDir, String logFile) throws IOException {
        new File(podLogPath + "/" + podUid + logDir).mkdirs();
        new File(podLogPath + "/" + podUid + logDir + "/" + logFile).createNewFile();
    }

    private static boolean deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            if (children != null) {
                for (String child : children) {
                    if (!deleteDirectory(new File(dir, child))) {
                        return false;
                    }
                }
            }
        }
        return dir.delete();
    }
}
