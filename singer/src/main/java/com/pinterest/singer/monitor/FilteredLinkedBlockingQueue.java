package com.pinterest.singer.monitor;

import java.nio.file.WatchEvent.Kind;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.annotations.VisibleForTesting;

/**
 * This queue only allows enqueuing of elements to allow deduplication/filtering
 * for events that are already present It uses an internal set to detect and
 * remove duplicates.
 */
public class FilteredLinkedBlockingQueue extends LinkedBlockingQueue<FileSystemEvent> {

  private static final long serialVersionUID = 1L;

  private Set<FileSystemEvent> uniqueEntries;
  private Set<Kind<?>> dedupedEventKinds;

  public FilteredLinkedBlockingQueue(Kind<?>... dedupedEventKinds) {
    this.dedupedEventKinds = new HashSet<>(Arrays.asList(dedupedEventKinds));
    this.uniqueEntries = new ConcurrentSkipListSet<>();
  }

  @Override
  public synchronized boolean add(FileSystemEvent e) {
    // only apply filtering to events that are no
    if (dedupedEventKinds.contains(e.event().kind())) {
      if (uniqueEntries.contains(e)) {
        // event is already there so we are going to drop the duplicate
        return false;
      } else {
        // if event is not present then add it to the set for tracking
        uniqueEntries.add(e);
      }
    }
    // run super add method
    return super.add(e);
  }

  @Override
  public synchronized FileSystemEvent take() throws InterruptedException {
    FileSystemEvent e = super.take();
    uniqueEntries.remove(e);
    return e;
  }

  @VisibleForTesting
  protected Set<FileSystemEvent> getUniqueEntries() {
    return uniqueEntries;
  }

}
