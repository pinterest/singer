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

import java.nio.file.Path;
import java.nio.file.WatchEvent;

public class FileSystemEvent {
  private Path logDir;
  private WatchEvent<?> event;

  public FileSystemEvent(Path path, WatchEvent<?> event) {
    this.logDir = path;
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
    return logDir.hashCode() * 31 + event.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FileSystemEvent) {
      FileSystemEvent fse = (FileSystemEvent) obj;
      return fse.logDir().equals(logDir)
          && ((Path) fse.event.context()).equals((Path) event.context())
          && (fse.event.kind().equals(event.kind()));
    }
    return false;
  }
}
