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
package com.pinterest.singer.monitor;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.Collection;
import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.common.errors.SingerLogException;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;

/**
 * Recursively watch POD's log directories to make sure we have valid log directories before logstreams can be
 * initialized.
 *
 * This class runs as a service continuously checks for new filesystem events, extrapolates which pod they are for and
 * finds if there is a match for singerlogconfig on these events to register logstreams for the same.
 */
public class RecursiveFSEventProcessor implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(RecursiveFSEventProcessor.class);
  private LogStreamManager lsm;

  public RecursiveFSEventProcessor(LogStreamManager lsm) {
    this.lsm = lsm;
  }

  @Override
  public void run() {
    while (true) {
      try {
        FileSystemEvent event = lsm.getRecursiveDirectoryWatcher().getEvent();
        try {
          processFileSystemEvent(event);
        } catch (Exception e) {
          LOG.error("Exception processing event in RecursiveProcessor", e);
        }
      } catch (InterruptedException e) {
        break;
      }
    }
  }

  /**
   * Check if the file system event is a create event
   */
  public void processFileSystemEvent(FileSystemEvent event) throws Exception {
    Path absolutePath = event.logDir();
    String podUid = extractPodUidFromPath(absolutePath,
        SingerSettings.getSingerConfig().getKubeConfig().getPodLogDirectory());

    WatchEvent.Kind<?> kind = event.event().kind();
    Path path = (Path) event.event().context();

    // resolve path because WatchService returns event Paths that are relative
    path = event.logDir().resolve(path);

    boolean isCreate = kind.equals(StandardWatchEventKinds.ENTRY_CREATE);
    if (isCreate) {
      evaluateAndRegisterLogStreamOrWatcher(path, podUid);
    }
  }

  /**
   * Extract Pod Uid from the supplied path, this is expecting the Kubernetes log directory convention for directory
   * names which is /<kube log dir>/<pod uid>/<log directory>
   *
   * e.g. /var/log/pods/pod11222/var/log/ thus the resulting pod id will be pod11222
   *
   * @return pod uid
   */
  public static String extractPodUidFromPath(Path absolutePath, String kubeLogDirectory) {
    kubeLogDirectory = new File(kubeLogDirectory).toPath().normalize().toString() + "/";
    String remainingPath = absolutePath.normalize().toAbsolutePath().toString().replace(kubeLogDirectory, "");
    // extract pod uid from path
    String podUid = remainingPath.contains("/") ? remainingPath.substring(0, remainingPath.indexOf("/"))
        : remainingPath;
    return podUid;
  }

  /**
   * Extracts the trailing path (suffix) from the pod log path.
   *
   * e.g. /var/log/pods/pod11222/var/xyz/ will return /var/xyz
   *
   * @return relativeLogPath
   */
  public static String extractRelativeLogPathFromAbsoluteEventPath(String podLogDirectory, Path path, String podUid) {
    podLogDirectory = new File(podLogDirectory).toPath().normalize().toString() + "/";
    String logConfigKey = path.normalize().toString();
    logConfigKey = logConfigKey.replace(podLogDirectory, "");
    logConfigKey = logConfigKey.replace(podUid, "");
    return logConfigKey;
  }

  /**
   * Checks if this path is a valid singerlogconfig directory; if so then it will register LogStreams.
   *
   * If there are any logconfigs with directory that has the supplied as prefix, it will attach watchers for those
   * directories recursively (for existing directories). So we should be able to capture any new directory creates and
   * any existing sub-directories that were created between the time FSEvent was triggered and now.
   *
   * e.g. SingerLogConfig /var/log/security/access.log, here directory config is /var/log/security
   *
   * This method will attach listeners if path for the following paths (method parameter): - /xyz/<podid>/var -
   * /xyz/<podid>/var/log
   *
   * And it will register logstream for /var/log/security
   */
  public void evaluateAndRegisterLogStreamOrWatcher(Path path, String podUid) {
    File file = path.toFile();

    if (file.getName().startsWith(".")) {
      // ignore dot files
      return;
    }

    LOG.info("New watcher event inside POD:" + podUid + " path:" + path);

    // check if there is a singer log configuration for this path
    // if yes then register logstreams for these configurations
    // if no then do recursive watching

    String logConfigKey = extractRelativeLogPathFromAbsoluteEventPath(
        SingerSettings.getSingerConfig().getKubeConfig().getPodLogDirectory(), path, podUid);

    Collection<SingerLogConfig> collection = SingerSettings.getLogConfigMap().get(logConfigKey);
    if (collection != null) {
      LOG.info("Found log config collection for pod:" + podUid + " and directory:" + path + " configsize:"
          + collection.size());
      try {
        lsm.initializeLogStreamForPod(podUid, collection);
      } catch (SingerLogException e) {
        LOG.error("Failed to initialize POD streams for pod:" + podUid, e);
      }
    } else {
      LOG.debug("No matching configurations found for directory");
    }

    // Check if we should register recursive watchers
    SortedMap<String, Collection<SingerLogConfig>> subMap = SingerSettings.getLogConfigMap().subMap(logConfigKey,
        logConfigKey + "/" + Character.MAX_VALUE);

    // only register new watchers if there are any log configurations for it's
    // subdirectories
    if (subMap != null && subMap.size() > 0 && !file.isFile()) {
      // register for recursive watch if this event wasn't for a file
      LOG.info("Registering recursive watch for path:" + path);
      try {
        lsm.getRecursiveDirectoryWatcher().registerPath(path);
      } catch (IOException e) {
        LOG.error("Problem with recursive directory watch for path:" + path, e);
      }
      // scan this directory for any missed events and attach watchers for those
      // directories
      listDirectoriesAndCallEvaluate(path, podUid);
    } else {
      LOG.warn("Ignoring path from recursive watch:" + path + " logconfigspaths:" + SingerSettings.getLogConfigMap()
          .keySet());
    }
  }

  /**
   * Lists directories under the path and triggers logstream registration check and watcher check.
   *
   * This method is basically refactored code from evaluateAndRegisterLogStreamOrWatcher for readability purpose.
   */
  public void listDirectoriesAndCallEvaluate(Path path, String podUid) {
    File[] directories = path.toFile().listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        return pathname.isDirectory();
      }
    });

    if (directories != null) {
      LOG.info("Found existing directories:" + directories.length + " for pod:" + podUid + " under path:" + path);
      for (File dir : directories) {
        evaluateAndRegisterLogStreamOrWatcher(dir.toPath(), podUid);
      }
    }
  }

}