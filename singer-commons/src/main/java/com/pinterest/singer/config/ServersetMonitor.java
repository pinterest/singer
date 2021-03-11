package com.pinterest.singer.config;

import com.google.common.collect.ImmutableSet;

public interface ServersetMonitor {

  public void onChange(ImmutableSet<Address> serviceInstances);
  
}
