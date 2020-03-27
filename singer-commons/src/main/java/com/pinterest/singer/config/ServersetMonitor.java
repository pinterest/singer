package com.pinterest.singer.config;

import com.google.common.collect.ImmutableSet;

import java.net.InetSocketAddress;

public interface ServersetMonitor {

  public void onChange(ImmutableSet<InetSocketAddress> serviceInstances);
  
}
