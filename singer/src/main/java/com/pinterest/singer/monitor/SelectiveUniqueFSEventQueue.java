/**
 * Copyright 2020 Pinterest, Inc.
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

import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;

public class SelectiveUniqueFSEventQueue extends LinkedBlockingQueue<FileSystemEvent> {

  private static final long serialVersionUID = 1L;
  private Set<FileSystemEvent> eventSet = ConcurrentHashMap.newKeySet();
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private WriteLock writeLock = lock.writeLock();
  private static final Set<WatchEvent.Kind<?>> DEDUPPED_EVENT_TYPES = new HashSet<>(
      Arrays.asList(StandardWatchEventKinds.ENTRY_MODIFY));

  public SelectiveUniqueFSEventQueue() {
  }

  @Override
  public boolean add(FileSystemEvent e) {
    writeLock.lock();
    try {
      OpenTsdbMetricConverter.incr("singer.selectivequeue.enqueue");
      if (DEDUPPED_EVENT_TYPES.contains(e.event().kind())) {
        if (!eventSet.add(e)) {
          OpenTsdbMetricConverter.incr("singer.selectivequeue.deduplication");
          return false;
        }
      }
      return super.add(e);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public FileSystemEvent take() throws InterruptedException {
    OpenTsdbMetricConverter.incr("singer.selectivequeue.dequeue");
    FileSystemEvent event = super.take();
    if (DEDUPPED_EVENT_TYPES.contains(event.event().kind())) {
      eventSet.remove(event);
    }
    return event;
  }

  @VisibleForTesting
  protected Set<FileSystemEvent> getEventSet() {
    return eventSet;
  }

}