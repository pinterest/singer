/**
 * Copyright 2026 Pinterest, Inc.
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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.pinterest.singer.SingerTestBase;
import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.monitor.LogStreamManager;
import com.pinterest.singer.thrift.configuration.FileNameMatchMode;
import com.pinterest.singer.thrift.configuration.KubeConfig;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.utils.TopicTemplateResolver;

/**
 * Tests for tailing container stdout logs from the kubelet pod log directory layout
 * (/var/log/pods/&lt;namespace&gt;_&lt;pod&gt;_&lt;uid&gt;/&lt;container&gt;/0.log) using a
 * wildcard logDir ("/*"). The SingerLog is registered under the literal wildcard path and
 * LogConfigUtils.findDirectories expands it, so each container directory must end up with
 * its own LogStream whose directory is concrete — that is what lets the topic template
 * resolver derive %{container} for per-container topic routing.
 */
public class TestContainerLogStreams {

    private SingerConfig config;
    private KubeConfig kubeConfig;
    private String podLogPath;
    private Path tempDir;

    // Pod directory name from pods-goodresponse.json: namespace_name_uid
    private static final String POD_NGINX_1 = "default_nginx-deployment-5c689d7589-abcde_12345678-1234-1234-1234-1234567890ab";

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
        // logConfigMap is not cleared by SingerSettings.reset(); without this, configs
        // accumulate across test methods and bleed into each other's assertions
        SingerSettings.getLogConfigMap().clear();
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

        tempDir = Files.createTempDirectory("container_log_test");
        podLogPath = tempDir.toAbsolutePath().toString();
        kubeConfig.setPodLogDirectory(podLogPath);
    }

    @After
    public void after() {
        TestKubeService.removePodsContext();
        SingerSettings.getFsMonitorMap().clear();
        SingerSettings.getLogConfigMap().clear();
        if (tempDir != null) {
            deleteDirectory(tempDir.toFile());
        }
        LogStreamManager.reset();
        KubeService.reset();
        PodMetadataFetcher.reset();
        SingerSettings.reset();
    }

    @Test
    public void testWildcardLogDirCreatesStreamPerContainerDirectory() throws Exception {
        SingerLogConfig logConfig = createLogConfig("container_logs", "/*", "0.log");
        config.setLogConfigs(Arrays.asList(logConfig));
        SingerSettings.getLogConfigMap().putAll(SingerSettings.loadLogConfigMap(config));

        createContainerLog(POD_NGINX_1, "web");
        createContainerLog(POD_NGINX_1, "sidecar");

        LogStreamManager lsm = LogStreamManager.getInstance();
        KubeService instance = KubeService.getInstance();
        instance.start();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        // the SingerLog is registered under the literal wildcard path
        assertTrue("wildcard path should be registered",
            lsm.getSingerLogPaths().containsKey(podLogPath + "/" + POD_NGINX_1 + "/*"));

        // findDirectories must have expanded the wildcard into one LogStream per
        // container directory, each with its concrete directory
        String webPath = podLogPath + "/" + POD_NGINX_1 + "/web";
        String sidecarPath = podLogPath + "/" + POD_NGINX_1 + "/sidecar";
        assertTrue("web container should have a log stream",
            lsm.getDirStreams().containsKey(webPath));
        assertTrue("sidecar container should have a log stream",
            lsm.getDirStreams().containsKey(sidecarPath));

        // end to end: each container's stream resolves to its own topic
        Set<String> resolvedTopics = new HashSet<>();
        for (String containerPath : new String[] { webPath, sidecarPath }) {
            Collection<LogStream> streams = lsm.getDirStreams().get(containerPath);
            assertEquals(1, streams.size());
            LogStream stream = streams.iterator().next();
            assertEquals(containerPath, stream.getLogDir());
            resolvedTopics.add(TopicTemplateResolver
                .resolve("logs_%{namespace}_%{container}", stream, podLogPath).get());
        }
        assertEquals(new HashSet<>(Arrays.asList("logs_default_web", "logs_default_sidecar")),
            resolvedTopics);

        instance.stop();
    }

    @Test
    public void testWildcardAndExactConfigsDoNotShadowEachOther() throws Exception {
        SingerLogConfig wildcardConfig = createLogConfig("all_containers", "/*", "0.log");
        SingerLogConfig exactConfig = createLogConfig("web_only", "/web", "0.log");
        config.setLogConfigs(Arrays.asList(wildcardConfig, exactConfig));
        SingerSettings.getLogConfigMap().putAll(SingerSettings.loadLogConfigMap(config));

        createContainerLog(POD_NGINX_1, "web");

        LogStreamManager lsm = LogStreamManager.getInstance();
        KubeService instance = KubeService.getInstance();
        instance.start();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        // before the matching fix, the wildcard entry shadowed the exact entry and the
        // exact config was never initialized
        assertTrue("wildcard config should be registered",
            lsm.getSingerLogPaths().containsKey(podLogPath + "/" + POD_NGINX_1 + "/*"));
        assertTrue("exact-directory config should be registered too",
            lsm.getSingerLogPaths().containsKey(podLogPath + "/" + POD_NGINX_1 + "/web"));

        instance.stop();
    }

    @Test
    public void testNonMatchingDirectoriesAreNotRegistered() throws Exception {
        SingerLogConfig exactConfig = createLogConfig("web_only", "/web", "0.log");
        config.setLogConfigs(Arrays.asList(exactConfig));
        SingerSettings.getLogConfigMap().putAll(SingerSettings.loadLogConfigMap(config));

        createContainerLog(POD_NGINX_1, "sidecar");

        LogStreamManager lsm = LogStreamManager.getInstance();
        KubeService instance = KubeService.getInstance();
        instance.start();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        assertFalse(lsm.getSingerLogPaths()
            .containsKey(podLogPath + "/" + POD_NGINX_1 + "/sidecar"));

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

    private void createContainerLog(String podUid, String container) throws IOException {
        new File(podLogPath + "/" + podUid + "/" + container).mkdirs();
        new File(podLogPath + "/" + podUid + "/" + container + "/0.log").createNewFile();
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
