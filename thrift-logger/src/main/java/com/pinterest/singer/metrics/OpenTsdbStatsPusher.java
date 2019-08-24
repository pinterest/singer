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

import com.twitter.ostrich.stats.Distribution;
import com.twitter.ostrich.stats.StatsSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Map;

import java.io.IOException;
import java.net.UnknownHostException;

/**
 * A daemon thread that periodically reads stats from Ostrich and sends them to OpenTSDB.
 *
 * The thread polls Ostrich every N milliseconds to get the counter, gauge and metric values since
 * the last interval (the very first interval is discarded, since its start time is unknown). It is
 * important that the intervals be as close to each other as possible (so they can be compared to
 * each other), so every effort is made to keep the intervals identical (but this isn't a real-time
 * system, so there are no guarantees).
 *
 * A {@link OpenTsdbMetricConverter} is used to convert from Ostrich stats to OpenTSDB stats. The
 * converter can be used to rename stats, add tags and even to modify the stat value. In addition,
 * the logic for converting Ostrich {@link Distribution}s to OpenTSDB stats must be done inside an
 * {@link OpenTsdbMetricConverter}.
 */
public class OpenTsdbStatsPusher extends StatsPusher {

  private static final Logger LOG = LoggerFactory.getLogger(OpenTsdbStatsPusher.class);
  private static final int RETRY_SLEEP_MS = 100;
  private static final int MIN_SOCKET_TIME_MS = 200;
  private OpenTsdbClient client;
  protected OpenTsdbClient.MetricsBuffer buffer;
  protected OpenTsdbMetricConverter converter;

  public OpenTsdbStatsPusher() {
    this.buffer = new OpenTsdbClient.MetricsBuffer();
  }
  
  @Override
  public void configure(String sourceHostname,
                        String metricsPrefix,
                        String destinationHost,
                        int destinationPort,
                        long pollMillis) throws IOException {
    super.configure(sourceHostname, metricsPrefix, destinationHost, destinationPort, pollMillis);
    this.client = new OpenTsdbClient(destinationHost, destinationPort);
    this.converter = new OpenTsdbMetricConverter(metricsPrefix, sourceHostname);
  }

  @SuppressWarnings("unchecked")
  protected void fillMetricsBuffer(StatsSummary summary, int epochSecs) {
    buffer.reset();
    OpenTsdbClient.MetricsBuffer buf = buffer;

    Map<String, Long> counters = (Map<String, Long>) (Map<String, ?>) summary.counters();
    Iterator<Tuple2<String, Long>> countersIter = counters.iterator();
    while (countersIter.hasNext()) {
      Tuple2<String, Long> tuple = countersIter.next();
      converter.convertCounter(tuple._1, epochSecs, tuple._2, buf);
    }

    Map<String, Double> gauges = (Map<String, Double>) (Map<String, ?>) summary.gauges();
    Iterator<Tuple2<String, Double>> gaugesIter = gauges.iterator();
    while (gaugesIter.hasNext()) {
      Tuple2<String, Double> tuple = gaugesIter.next();
      converter.convertGauge(tuple._1, epochSecs, (float) tuple._2.doubleValue(), buf);
    }

    Map<String, Distribution> metrics = summary.metrics();
    Iterator<Tuple2<String, Distribution>> metricsIter = metrics.iterator();
    while (metricsIter.hasNext()) {
      Tuple2<String, Distribution> tuple = metricsIter.next();
      converter.convertMetric(tuple._1, epochSecs, tuple._2, buf);
    }
  }

  private void logOstrichStats(int epochSecs) {
    LOG.debug("Ostrich Metrics {}: \n{}", epochSecs, buffer.toString());
  }

  @Override
  public long sendMetrics(boolean retryOnFailure)
      throws InterruptedException, UnknownHostException {
    long startTimeinMillis = System.currentTimeMillis();
    long end = startTimeinMillis + pollMillis;

    StatsSummary summary = statsListener.get();
    int epochSecs = (int) (startTimeinMillis / 1000L);
    fillMetricsBuffer(summary, epochSecs);
    if (LOG.isDebugEnabled()) {
      logOstrichStats(epochSecs);
    }

    while (true) {
      try {
        client.sendMetrics(buffer);
        break;
      } catch (Exception ex) {
        LOG.warn("Failed to send stats to OpenTSDB, will retry up to next interval", ex);
        if (!retryOnFailure) {
          break;
        }
        // re-initiaize OpenTsdbClient before retrying
        client = new OpenTsdbClient(destinationHost, destinationPort);
      }
      if (end - System.currentTimeMillis() < RETRY_SLEEP_MS + MIN_SOCKET_TIME_MS) {
        LOG.error("Failed to send epoch {} to OpenTSDB, moving to next interval", epochSecs);
        break;
      }
      Thread.sleep(RETRY_SLEEP_MS);
    }

    return System.currentTimeMillis() - startTimeinMillis;
  }

  /**
   * @return the buffer
   */
  public OpenTsdbClient.MetricsBuffer getBuffer() {
    return buffer;
  }
  
  public void setConverter(OpenTsdbMetricConverter converter) {
    this.converter = converter;
  }
  
}