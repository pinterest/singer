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
package com.pinterest.singer.metrics;

import com.google.common.collect.Maps;
import com.twitter.ostrich.admin.AdminHttpService;
import com.twitter.ostrich.admin.AdminServiceFactory;
import com.twitter.ostrich.admin.CustomHttpHandler;
import com.twitter.ostrich.admin.RuntimeEnvironment;
import com.twitter.ostrich.admin.StatsFactory;
import com.twitter.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.Map$;
import scala.collection.immutable.List$;
import scala.util.matching.Regex;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class OstrichAdminService {

  private static final Logger LOG = LoggerFactory.getLogger(OstrichAdminService.class);
  private final int port;
  private final Map<String, CustomHttpHandler> customHttpHandlerMap = Maps.newHashMap();

  public OstrichAdminService(int port) {
    this.port = port;
  }

  public void addHandler(String path, CustomHttpHandler handler) {
    this.customHttpHandlerMap.put(path, handler);
  }

  @SuppressWarnings("restriction")
  public void start() {
    try {
      Properties properties = new Properties();
      properties.load(this.getClass().getResource("build.properties").openStream());
      LOG.info("build.properties build_revision: {}",
          properties.getProperty("build_revision", "unknown"));
    } catch (Throwable t) {
      LOG.warn("Failed to load properties from build.properties");
    }
    Duration[] defaultLatchIntervals = {Duration.apply(1, TimeUnit.MINUTES)};
    Iterator<Duration> durationIterator = Arrays.asList(defaultLatchIntervals).iterator();

    AdminServiceFactory adminServiceFactory = new AdminServiceFactory(
            this.port,
            20,
            List$.MODULE$.<StatsFactory>empty(),
            Option.<String>empty(),
            List$.MODULE$.<Regex>empty(),
            Map$.MODULE$.<String, CustomHttpHandler>empty(),
            JavaConversions.asScalaIterator(durationIterator).toList());

    RuntimeEnvironment runtimeEnvironment = new RuntimeEnvironment(this);
    AdminHttpService service = adminServiceFactory.apply(runtimeEnvironment);
    for (Map.Entry<String, CustomHttpHandler> entry : this.customHttpHandlerMap.entrySet()) {
      service.httpServer().createContext(entry.getKey(), entry.getValue());
    }
  }

}
