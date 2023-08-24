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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;

import org.junit.Test;

public class TestSelectiveUniqueFSEventQueue {

  @Test
  public void testAddAndTakeToQueue() throws InterruptedException {
    SelectiveUniqueFSEventQueue queue = new SelectiveUniqueFSEventQueue();
    Path dir = new File("target").toPath();
    queue.add(new FileSystemEvent(dir, new TestWatchEvent(StandardWatchEventKinds.ENTRY_CREATE,
        new File("target/test1").toPath())));
    assertEquals(0, queue.getEventSet().size());
    assertTrue(
        queue.add(new FileSystemEvent(dir, new TestWatchEvent(StandardWatchEventKinds.ENTRY_MODIFY,
            new File("target/test1").toPath()))));
    assertEquals(1, queue.getEventSet().size());
    assertFalse(
        queue.add(new FileSystemEvent(dir, new TestWatchEvent(StandardWatchEventKinds.ENTRY_MODIFY,
            new File("target/test1").toPath()))));
    assertEquals(1, queue.getEventSet().size());
    assertEquals(2, queue.size());
    assertTrue(
        queue.add(new FileSystemEvent(dir, new TestWatchEvent(StandardWatchEventKinds.ENTRY_CREATE,
            new File("target/test1").toPath()))));
    assertEquals(1, queue.getEventSet().size());
    assertEquals(3, queue.size());

    for (int i = 0; i < 3; i++) {
      queue.take();
    }
    assertEquals(0, queue.getEventSet().size());
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static class TestWatchEvent implements WatchEvent<Object> {

    private Kind<Object> kind;
    private Object context;

    public TestWatchEvent(Kind kind, Object context) {
      this.kind = kind;
      this.context = context;
    }

    @Override
    public Kind<Object> kind() {
      return kind;
    }

    @Override
    public int count() {
      return 1;
    }

    @Override
    public Object context() {
      return context;
    }

  }
}