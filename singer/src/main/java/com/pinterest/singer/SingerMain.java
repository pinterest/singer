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
package com.pinterest.singer;

import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.config.DirectorySingerConfigurator;
import com.pinterest.singer.config.SingerConfigurator;
import com.pinterest.singer.heartbeat.HeartbeatGenerator;
import com.pinterest.singer.metrics.StatsPusher;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.metrics.OstrichAdminService;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.utils.SingerUtils;
import com.pinterest.singer.writer.KafkaProducerManager;

import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class SingerMain {

  private static final String SINGER_METRICS_PREFIX = "singer";
  private static final Logger LOG = LoggerFactory.getLogger(SingerMain.class);
  private static final int STATS_PUSH_INTERVAL_IN_MILLISECONDS = 10 * 1000;
  protected static final String hostName = SingerUtils.getHostname();
  private static  StatsPusher statsPusher = null;
  private static String singerPath = "";

  static class SingerCleanupThread extends Thread {

    @Override
    public void run() {
      try {
        if (SingerSettings.getLogMonitor() != null) {
          SingerSettings.getLogMonitor().stop();
        } else {
          LOG.error("LogMonitor is not initialized properly.");
        }
      } catch (Throwable t) {
        LOG.error("Shutdown failure: log monitor : ", t);
      }

      try {
        if (SingerSettings.heartbeatGenerator != null) {
          SingerSettings.heartbeatGenerator.stop();
        }
      } catch (Throwable t) {
        LOG.error("Shutdown failure: heartbeat generator : ", t);
      }
      
      try {
        if (SingerSettings.getKafkaProducerMonitorThread() != null) {
           SingerSettings.getKafkaProducerMonitorThread().interrupt();
        }
      }catch(Throwable t) {
        LOG.error("Shutdown error: kafka producer metrics monitor : ", t);
      }

      try {
        KafkaProducerManager.shutdown();
      } catch (Throwable t) {
        LOG.error("Shutdown failure: kafka producers : ", t);
      }

      try{
        if (SingerSettings.getLoggingAuditClient() != null){
          SingerSettings.getLoggingAuditClient().close();
        }
      } catch (Throwable t){
        LOG.error("Shutdown failure: LoggingAudit client : ", t);
      }

      try {
        OpenTsdbMetricConverter.incr("singer.shutdown", 1);
        if (statsPusher!= null) {
          statsPusher.sendMetrics(false);
        } else {
          LOG.error("metricsPusher was not initialized properly.");
        }
      } catch (Throwable t) {
        LOG.error("Shutdown failure: metrics : ", t);
      }
    }
  }

  public static void startOstrichService(SingerConfig singerConfig) {
    // do not start ostrich if Ostrich server is disabled 
    if (System.getenv(SingerMetrics.DISABLE_SINGER_OSTRICH) == null) {
      OstrichAdminService ostrichService = new OstrichAdminService(singerConfig.getOstrichPort());
      ostrichService.start();
    }
    // enable high granularity metrics we are running in canary
    if (singerConfig.isSetStatsPusherHostPort()) {
      LOG.info("Starting the stats pusher");
      try {
        @SuppressWarnings("unchecked")
        Class<StatsPusher> pusherClass = (Class<StatsPusher>) Class.forName(singerConfig.getStatsPusherClass());
        statsPusher = pusherClass.newInstance();
        HostAndPort pushHostPort = HostAndPort.fromString(singerConfig.getStatsPusherHostPort());
        // allows hostname to be overridden based on environment
        statsPusher.configure(SingerSettings.getEnvironment().getHostname()
            , SINGER_METRICS_PREFIX
            , pushHostPort.getHost()
            , pushHostPort.getPort()
            , STATS_PUSH_INTERVAL_IN_MILLISECONDS);
        statsPusher.start();
        LOG.info("Stats pusher started!");
      } catch (Throwable t) {
        // pusher fail is OK, do
        LOG.error("Exception when starting stats pusher: ", t);
      }
    }
  }

  public static void validateConfig() throws Exception {
    SingerConfigurator singerConfigurator = new DirectorySingerConfigurator(singerPath);
    singerConfigurator.parseSingerConfig();
  }

  public static boolean parseValidateArgs(String[] args) throws Exception {
    for (int i=0; i < args.length; i++) {
      if (args[i].toLowerCase().equals("-validateconfig")) {
        if (i+1 < args.length) {
          singerPath = args[i+1];
          return true;
        } else {
          throw new Exception("No config file specified. The correct usage of the " +
                  "flag is -validateConfig [filePath]");
        }
      }
    }
    return false;
  }

  public static void main(String[] args) {
    LOG.warn("Starting Singer logging agent.");
    try {
      boolean validateConfig = parseValidateArgs(args);

      if (validateConfig) {
        validateConfig();
      } else {
        Runtime.getRuntime().addShutdownHook(new SingerCleanupThread());
        String configDir = System.getProperty("singer.config.dir");
        String propertiesFile = System.getProperty("config");
        SingerConfig singerConfig = SingerUtils.loadSingerConfig(configDir, propertiesFile, true);
        if (singerConfig.isHeartbeatEnabled()) {
          SingerSettings.heartbeatGenerator = new HeartbeatGenerator(singerConfig);
        }
        SingerSettings.initialize(singerConfig);
        startOstrichService(singerConfig);
      }
    } catch (Throwable t) {
      LOG.error("Singer failed.", t);
      System.exit(1);
    }
  }
}