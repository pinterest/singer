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
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.pinterest.singer.SingerTestBase;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.common.errors.SingerLogException;
import com.pinterest.singer.monitor.FileSystemMonitor;
import com.pinterest.singer.monitor.LogStreamManager;
import com.pinterest.singer.thrift.configuration.FileNameMatchMode;
import com.pinterest.singer.thrift.configuration.KubeConfig;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;

import com.twitter.ostrich.stats.Stats;

public class TestPodLogCycle {

    private SingerConfig config;
    private KubeConfig kubeConfig;
    private String podLogPath;

    @Before
    public void before() throws ClassNotFoundException, InvocationTargetException, IllegalAccessException,
            NoSuchMethodException, IOException, SingerLogException {

        LogStreamManager.getInstance().getSingerLogPaths().clear();
        SingerSettings.getFsMonitorMap().clear();
        LogStreamManager.reset();
        KubeService.reset();
        SingerSettings.reset();
        
        SingerSettings.setBackgroundTaskExecutor(Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).build()));

        config = new SingerConfig();
        config.setKubernetesEnabled(true);
        SingerSettings.setSingerConfig(config);

        kubeConfig = new KubeConfig();
        config.setKubeConfig(kubeConfig);

        podLogPath = new File("").getAbsolutePath() + "/target/pods";
        kubeConfig.setPodLogDirectory(podLogPath);
        kubeConfig.setIgnorePodDirectory("/Ignore");
        delete(new File(podLogPath));
        new File(podLogPath).mkdirs();

        System.out.println("Creating pod parent directory:" + podLogPath);
    }

    @After
    public void after() {
        LogStreamManager.getInstance().getSingerLogPaths().clear();
        SingerSettings.getFsMonitorMap().clear();
        delete(new File(podLogPath));
        LogStreamManager.reset();
        KubeService.reset();
        SingerSettings.reset();
    }

    @Test
    public void testExistingPodDetection() throws InterruptedException, SingerLogException, IOException {
        SingerLogConfig logConfig1 = new SingerLogConfig();
        logConfig1.setLogDir("/var/log");
        logConfig1.setFilenameMatchMode(FileNameMatchMode.PREFIX);
        logConfig1.setName("test1");
        logConfig1.setLogStreamRegex("access.log");

        List<SingerLogConfig> logConfigs = Arrays.asList(logConfig1);
        config.setLogConfigs(logConfigs);
        SingerSettings.getLogConfigMap().putAll(SingerSettings.loadLogConfigMap(config));

        new File(podLogPath + "/a1223-1111-2222-3333/var/log").mkdirs();
        new File(podLogPath + "/a1223-1111-2222-3333/var/log/access.log").createNewFile();

        // This should be ignored
        new File(podLogPath + "/b2334-1111-2222-3333/Ignore").mkdirs();
        new File(podLogPath + "/b2334-1111-2222-3333/var/log").mkdirs();
        new File(podLogPath + "/b2334-1111-2222-3333/var/log/access.log").createNewFile();

        LogStreamManager lsm = LogStreamManager.getInstance();
        KubeService instance = KubeService.getInstance();
        instance.start();
        Thread.sleep(1000);

        assertEquals(1, instance.getActivePodSet().size());
        assertEquals(1, lsm.getSingerLogPaths().size());
        assertEquals(1, SingerSettings.getFsMonitorMap().size());
        assertTrue(SingerSettings.getFsMonitorMap().containsKey("a1223-1111-2222-3333"));
        assertTrue(LogStreamManager.getInstance().getSingerLogPaths()
                .containsKey(podLogPath + "/a1223-1111-2222-3333/var/log"));

        FileSystemMonitor fsm = SingerSettings.getFsMonitorMap().get("a1223-1111-2222-3333");
        assertEquals(fsm, SingerSettings.getOrCreateFileSystemMonitor("a1223-1111-2222-3333"));

        // tear down
        instance.stop();
        fsm.stop();

        delete(new File(podLogPath + "/a1223-1111-2222-3333"));
    }

    @Test
    public void testNewPodDetection() throws InterruptedException, SingerLogException, IOException {
        SingerLogConfig logConfig2 = new SingerLogConfig();
        logConfig2.setLogDir("/var/log");
        logConfig2.setFilenameMatchMode(FileNameMatchMode.PREFIX);
        logConfig2.setName("test2");
        logConfig2.setLogStreamRegex("access2.log");

        List<SingerLogConfig> logConfigs = Arrays.asList(logConfig2);
        SingerSettings.getSingerConfig().setLogConfigs(logConfigs);
        SingerSettings.initializeConfigMap(config);

        LogStreamManager lsm = LogStreamManager.getInstance();
        KubeService instance = KubeService.getInstance();
        instance.start();

        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        assertEquals("Shouldn't have found any pods:" + Arrays.toString(new File(podLogPath).list()), 0,
                instance.getActivePodSet().size());
        assertEquals(0, lsm.getSingerLogPaths().size());

        new File(podLogPath + "/b2121-1111-2222-3333/var/log").mkdirs();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        File file = new File(podLogPath + "/b2121-1111-2222-3333/var/log/access2.log");
        file.createNewFile();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);
        
        PrintWriter pr = new PrintWriter(file);
        pr.println("testdata");
        pr.close();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS * 2);

        assertEquals(1, instance.getActivePodSet().size());
        assertEquals(1, lsm.getSingerLogPaths().size());
        assertEquals(1, SingerSettings.getFsMonitorMap().size());
        assertTrue("failed:" + SingerSettings.getFsMonitorMap(),
                SingerSettings.getFsMonitorMap().containsKey("b2121-1111-2222-3333"));
        
        assertEquals(1, lsm.getSingerLogPaths().size());

        instance.stop();
        LogStreamManager.reset();
    }

    @Test
    public void testPodIgnore() throws InterruptedException, SingerLogException, IOException {
        SingerLogConfig logConfig2 = new SingerLogConfig();
        logConfig2.setLogDir("/var/log");
        logConfig2.setFilenameMatchMode(FileNameMatchMode.PREFIX);
        logConfig2.setName("test2");
        logConfig2.setLogStreamRegex("access2.log");

        List<SingerLogConfig> logConfigs = Arrays.asList(logConfig2);
        SingerSettings.getSingerConfig().setLogConfigs(logConfigs);
        SingerSettings.initializeConfigMap(config);

        LogStreamManager lsm = LogStreamManager.getInstance();
        KubeService instance = KubeService.getInstance();
        instance.start();

        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        assertEquals("Shouldn't have found any pods:" + Arrays.toString(new File(podLogPath).list()), 0,
            instance.getActivePodSet().size());
        assertEquals(0, lsm.getSingerLogPaths().size());

        new File(podLogPath + "/b2121-1111-2222-3333/Ignore").mkdirs();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        new File(podLogPath + "/b2121-1111-2222-3333/var/log").mkdirs();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        File file = new File(podLogPath + "/b2121-1111-2222-3333/var/log/access2.log");
        file.createNewFile();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        assertEquals(0, instance.getActivePodSet().size());
        assertEquals(0, lsm.getSingerLogPaths().size());
        assertEquals(0, SingerSettings.getFsMonitorMap().size());
        assertFalse("failed:" + SingerSettings.getFsMonitorMap(),
            SingerSettings.getFsMonitorMap().containsKey("b2121-1111-2222-3333"));

        instance.stop();
        LogStreamManager.reset();
    }
    @Test
    public void testPodExternalPodDeletion() throws InterruptedException, IOException {
        SingerLogConfig logConfig2 = new SingerLogConfig();
        logConfig2.setLogDir("/var/log");
        logConfig2.setFilenameMatchMode(FileNameMatchMode.PREFIX);
        logConfig2.setName("test3");
        logConfig2.setLogStreamRegex("adlogs.log");

        LogStreamManager lsm = LogStreamManager.getInstance();
        KubeService instance = KubeService.getInstance();
        instance.start();

        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        assertEquals("Found pods:" + Arrays.toString(new File(podLogPath).list()), 0,
                instance.getActivePodSet().size());
        assertEquals(0, lsm.getSingerLogPaths().size());

        new File(podLogPath + "/c2121-1111-2222-3333/var/log").mkdirs();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        new File(podLogPath + "/c2121-1111-2222-3333/var/log/access2.log").createNewFile();
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        assertEquals(1, instance.getActivePodSet().size());

        delete(new File(podLogPath + "/c2121-1111-2222-3333/var/log"));
        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS * 2);

        instance.stop();
        LogStreamManager.reset();
    }
    
    @Test
    public void testStatsUpdate() {
      // note this needs to be initialized
      Stats.setGauge(SingerMetrics.POD_DELETION_TIME_ELAPSED, 0);
      
      for (int i=0; i<1000; i++) {
        Double timeElapsed = ((Double)Stats.getGauge(SingerMetrics.POD_DELETION_TIME_ELAPSED).get());
        long maxElapsedTime = i;
        Stats.setGauge(SingerMetrics.POD_DELETION_TIME_ELAPSED, Math.max(maxElapsedTime, timeElapsed));
      }
      assertEquals(999, ((Double)Stats.getGauge(SingerMetrics.POD_DELETION_TIME_ELAPSED).get()), 0);
    }

  @Test
  public void testExistingPodDetectionWithMultipleDirectoriesAndWildcard()
      throws InterruptedException, SingerLogException, IOException {
    // Standard Config
    SingerLogConfig logConfig1 = new SingerLogConfig();
    logConfig1.setLogDir("/var/logs, /mnt/logs");
    logConfig1.setFilenameMatchMode(FileNameMatchMode.PREFIX);
    logConfig1.setName("test1");
    logConfig1.setLogStreamRegex("access.log");

    // Regex Config
    SingerLogConfig logConfig2 = new SingerLogConfig();
    logConfig2.setLogDir("/var, /var/logs/dir*/test");
    logConfig2.setFilenameMatchMode(FileNameMatchMode.PREFIX);
    logConfig2.setName("test2");
    logConfig2.setLogStreamRegex("access.log");

    // Add both log configurations
    List<SingerLogConfig> logConfigs = Arrays.asList(logConfig1, logConfig2);
    config.setLogConfigs(logConfigs);
    SingerSettings.getLogConfigMap().putAll(SingerSettings.loadLogConfigMap(config));

    // Regex-based directory creation
    new File(podLogPath + "/a1223-1111-2222-3333/var/logs/dir1/test").mkdirs();
    new File(podLogPath + "/a1223-1111-2222-3333/var/logs/dir2/test").mkdirs();
    new File(podLogPath + "/a1223-1111-2222-3333/var/logs/example/test").mkdirs();
    new File(podLogPath + "/a1223-1111-2222-3333/mnt/logs").mkdirs();
    new File(podLogPath + "/a1223-1111-2222-3333/var/logs/access.log").createNewFile();
    new File(podLogPath + "/a1223-1111-2222-3333/var/access.log").createNewFile();
    new File(podLogPath + "/a1223-1111-2222-3333/mnt/logs/access.log").createNewFile();
    new File(podLogPath + "/a1223-1111-2222-3333/var/logs/dir1/test/access.log").createNewFile();
    new File(podLogPath + "/a1223-1111-2222-3333/var/logs/dir2/test/access.log").createNewFile();
    new File(podLogPath + "/a1223-1111-2222-3333/var/logs/example/test/access.log").createNewFile();
    Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

    // Start the services
    LogStreamManager lsm = LogStreamManager.getInstance();
    KubeService instance = KubeService.getInstance();
    instance.start();
    Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

    // Assert standard path detection
    assertEquals(1, instance.getActivePodSet().size());
    System.out.println("Log stream paths: " + lsm.getSingerLogPaths());
    assertEquals(4, lsm.getSingerLogPaths().size()); // Now includes regex matches
    assertEquals(1, SingerSettings.getFsMonitorMap().size());
    assertTrue(SingerSettings.getFsMonitorMap().containsKey("a1223-1111-2222-3333"));
    assertTrue(LogStreamManager.getInstance().getSingerLogPaths()
        .containsKey(podLogPath + "/a1223-1111-2222-3333/var/logs/dir*/test"));
    assertTrue(LogStreamManager.getInstance().getSingerLogPaths()
        .containsKey(podLogPath + "/a1223-1111-2222-3333/mnt/logs"));

    // Tear down
    FileSystemMonitor fsm = SingerSettings.getFsMonitorMap().get("a1223-1111-2222-3333");
    assertEquals(fsm, SingerSettings.getOrCreateFileSystemMonitor("a1223-1111-2222-3333"));
    instance.stop();
    fsm.stop();
    delete(new File(podLogPath + "/a1223-1111-2222-3333"));
  }


  @Test
  public void testNewPodDetectionWithMultipleDirectoriesAndWildcards() throws InterruptedException, SingerLogException, IOException {
    // Standard Config
    SingerLogConfig logConfig1 = new SingerLogConfig();
    logConfig1.setLogDir("/var/log");
    logConfig1.setFilenameMatchMode(FileNameMatchMode.PREFIX);
    logConfig1.setName("test1");
    logConfig1.setLogStreamRegex("access.log");

    // Regex Config
    SingerLogConfig logConfig2 = new SingerLogConfig();
    logConfig2.setLogDir("/var/logs/*");
    logConfig2.setFilenameMatchMode(FileNameMatchMode.PREFIX);
    logConfig2.setName("test2");
    logConfig2.setLogStreamRegex("access2.log");

    // Add both log configurations
    List<SingerLogConfig> logConfigs = Arrays.asList(logConfig1, logConfig2);
    config.setLogConfigs(logConfigs);
    SingerSettings.getLogConfigMap().putAll(SingerSettings.loadLogConfigMap(config));

    // Start the services
    LogStreamManager lsm = LogStreamManager.getInstance();
    KubeService instance = KubeService.getInstance();
    instance.start();
    Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS * 2);

    assertEquals("Shouldn't have found any pods:" + Arrays.toString(new File(podLogPath).list()), 0,
        instance.getActivePodSet().size());
    assertEquals(0, lsm.getSingerLogPaths().size());

    // Create new directory to trigger pod detection
    new File(podLogPath + "/b2121-1111-2222-3333/var/logs/happy").mkdirs();
    new File(podLogPath + "/b2121-1111-2222-3333/var/log").mkdirs();
    Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

    File file1 = new File(podLogPath + "/b2121-1111-2222-3333/var/logs/happy/access2.log");
    file1.createNewFile();
    File file2 = new File(podLogPath + "/b2121-1111-2222-3333/var/log/access.log");
    file2.createNewFile();
    Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

    try (PrintWriter pr = new PrintWriter(file1)) {
      pr.println("testdata");
    }
    try (PrintWriter pr = new PrintWriter(file2)) {
      pr.println("testdata");
    }

    Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS * 2);

    assertEquals(1, instance.getActivePodSet().size());
    assertEquals(2, lsm.getSingerLogPaths().size());
    assertTrue(SingerSettings.getFsMonitorMap().containsKey("b2121-1111-2222-3333"));
    assertTrue(LogStreamManager.getInstance().getSingerLogPaths().containsKey(podLogPath + "/b2121-1111-2222-3333/var/logs/*"));
    assertTrue(LogStreamManager.getInstance().getSingerLogPaths().containsKey(podLogPath + "/b2121-1111-2222-3333/var/log"));

    // Tear down
    instance.stop();
    LogStreamManager.reset();
  }

    @Test
    public void testPodDirectoryDeletion() throws InterruptedException, IOException, SingerLogException {
        // Enable direct pod directory cleanup
        kubeConfig.setEnablePodLogDirectoryCleanup(true);
        kubeConfig.setDeletionCheckIntervalInSeconds(1);
        kubeConfig.setDefaultDeletionTimeoutInSeconds(1);

        SingerLogConfig logConfig = new SingerLogConfig();
        logConfig.setLogDir("/var/log");
        logConfig.setFilenameMatchMode(FileNameMatchMode.PREFIX);
        logConfig.setName("testDeletion");
        logConfig.setLogStreamRegex("test.log");

        List<SingerLogConfig> logConfigs = Arrays.asList(logConfig);
        config.setLogConfigs(logConfigs);
        SingerSettings.getLogConfigMap().putAll(SingerSettings.loadLogConfigMap(config));

        LogStreamManager lsm = LogStreamManager.getInstance();
        KubeService instance = KubeService.getInstance();
        instance.start();

        Thread.sleep(SingerTestBase.FILE_EVENT_WAIT_TIME_MS);

        // Create pod directory with files
        String podUid = "delete-test-pod-123";
        File podDirectory = new File(podLogPath + "/" + podUid);
        podDirectory.mkdirs();
        new File(podDirectory + "/var/log").mkdirs();
        File testFile = new File(podDirectory + "/var/log/test.log");
        testFile.createNewFile();

        assertTrue("Pod directory should exist", podDirectory.exists());
        assertTrue("Test file should exist", testFile.exists());

        // Simulate pod deletion - this should delete the directory directly, not create dot file
        lsm.podDeleted(podUid);

        // Wait for background deletion task
        Thread.sleep(3000);

        // Verify directory was deleted (not just dot file created)
        assertFalse("Pod directory should be deleted", podDirectory.exists());
        assertFalse("Test file should be deleted", testFile.exists());

        // Verify no dot file was created (old behavior)
        File dotFile = new File(podLogPath + "/." + podUid);
        assertFalse("Dot file should not be created (old behavior)", dotFile.exists());

        instance.stop();
        LogStreamManager.reset();
    }

    /*
     * Copied from
     * https://github.com/srotya/sidewinder/blob/development/core/src/main/java/com/
     * srotya/sidewinder/core/utils/MiscUtils.java under Apache 2.0 license
     */
    public static boolean delete(File file) {
        if (file.isDirectory()) {
            // directory is empty, then delete it
            if (file.list().length == 0) {
                return file.delete();
            } else {
                // list all the directory contents
                String files[] = file.list();
                boolean result = false;
                for (String temp : files) {
                    // construct the file structure
                    File fileDelete = new File(file, temp);
                    // recursive delete
                    result = delete(fileDelete);
                    if (!result) {
                        return false;
                    }
                }
                // check the directory again, if empty then delete it
                if (file.list().length == 0) {
                    file.delete();
                }
                return result;
            }
        } else {
            // if file, then delete it
            return file.delete();
        }
    }
}
