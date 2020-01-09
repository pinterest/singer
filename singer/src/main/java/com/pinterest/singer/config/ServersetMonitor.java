package com.pinterest.singer.config;

import java.net.InetSocketAddress;

import com.google.common.collect.ImmutableSet;

public interface ServersetMonitor {

  public void onChange(ImmutableSet<InetSocketAddress> serviceInstances);
  
}
