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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.WatchEvent;

public class FileSystemEvent {

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemEvent.class);
  private Path logDir;
  private WatchEvent<?> event;

  public FileSystemEvent(Path logDir, WatchEvent<?> event) {
    this.logDir = logDir;
    this.event = event;
  }

  public Path logDir() {
    return this.logDir;
  }

  public WatchEvent<?> event() {
    return this.event;
  }

  @Override
  public int hashCode() {
    int dirHash = logDir != null ? logDir().hashCode() : 0;
    int eventHash = event.context() != null ? event.context().hashCode() : 0;
    return 31 * dirHash + eventHash;
  }

  public boolean equals(Object obj) {
    if ((obj != null) && (obj instanceof FileSystemEvent)) {
      FileSystemEvent v = ((FileSystemEvent) obj);
      try {
        return logDir.equals(v.logDir) && event.context().equals(v.event.context())
            && event.kind().equals(v.event.kind());
      } catch (NullPointerException e) {
        LOG.error("Caught NPE when comparing events");
        LOG.error("LogDir: " + logDir);
        LOG.error("event: " + event);
        LOG.error("v.event.context(): " + v.event.context());
        LOG.error("v.logDir: " + v.logDir);
        return false;
      }
    }
    return false;
  }
}
