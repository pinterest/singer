package com.pinterest.singer.writer.memq.producer.http;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class DaemonThreadFactory implements ThreadFactory {

  private String basename;
  private AtomicInteger counter;

  public DaemonThreadFactory(String basename) {
    this.basename = basename;
    this.counter = new AtomicInteger();
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread th = new Thread(r);
    th.setDaemon(true);
    th.setName(basename + "-" + counter.getAndIncrement());
    return th;
  }
}