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
package com.pinterest.singer.common;

import com.pinterest.singer.utils.SingerUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.twitter.ostrich.stats.Distribution;
import com.twitter.ostrich.stats.Stats;
import com.twitter.ostrich.stats.StatsSummary;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Map;

import java.lang.management.ManagementFactory;
import java.util.TreeMap;

/**
 * This class captures the basic info for Singer heartbeat.
 *
 * {
 *   "hostname": "devhost",
 *   "jvmuptime": "240649",
 *   "kafkaWrites": "{\"auth_log\":\"59\"}",
 *   "latency":
 *      "{\"metrics.authlog\":
 *          \"{\\\"average\\\":1800,\\\"count\\\":24,\\\"maximum\\\":9498,
 *             \\\"minimum\\\":0,\\\"p50\\\":704,\\\"p90\\\":7775,\\\"p95\\\":8594,
 *             \\\"p99\\\":9498,\\\"p999\\\":9498,\\\"p9999\\\":9498,\\\"sum\\\":43222}\"}",
 *   "logstreams": "",
 *   "numLogStreams": "1",
 *   "numStuckLogStreams": "0",
 *   "processorExceptions": "0",
 *   "timestamp": "1472626620005",
 *   "version": "0.6.0"
 * }
 *
 * The following is the defintions of counters, metrics, and gauges in Scala.
 *   counters: Map[String, Long],
 *   metrics: Map[String, Distribution],
 *   gauges: Map[String, Double],
 */
public class SingerStatus {

  private static final Logger LOG = LoggerFactory.getLogger(SingerStatus.class);
  /**
   * The singer version number. This value need to be changed when a new version rolls out.
   */
  public static String SINGER_VERSION;
  private static final String VERSION_KEY = "version";
  private static final String HOSTNAME_KEY = "hostname";
  private static final String JVM_UPTIME_KEY = "jvmuptime";
  private static final String TIMESTAMP_KEY = "timestamp";
  private static final String PROCESSOR_EXCEPTIONS_KEY = "processorExceptions";
  private static final String NUM_LOG_STREAMS_KEY = "numLogStreams";
  private static final String NUM_STUCK_LOG_STREAMS_KEY = "numStuckLogStreams";
  private static final String KAFKA_WRITES_KEY = "kafkaWrites";
  private static final String LATENCY_KEY = "latency";
  private static final String CURRENT_LATENCY_KEY = "currentLatency";
  private static final String SKIPPED_BYTES_KEY = "skippedBytes";

  private final String version;
  public final String hostName;

  private final long timestamp;
  private final long jvmUptime;
  private final long numExceptions;
  private final long numLogStreams;
  private final long numStuckLogStreams;
  private final TreeMap<String, String> kafkaWrites = new TreeMap<>();
  private final TreeMap<String, String> latency = new TreeMap<>();
  private final TreeMap<String, String> skippedBytes = new TreeMap<>();
  private final double currentLatency;

  {
    String versionString;
    try {
      final Properties properties = new Properties();
      InputStream inputStream = SingerStatus.class.getResourceAsStream("/build.properties");
      properties.load(inputStream);
      versionString = properties.getProperty("version");
    } catch (IOException e) {
      versionString = "Unknown";
    }
    SINGER_VERSION = versionString;
  }

  public SingerStatus(long ts) {
    this.version = SINGER_VERSION;
    this.timestamp = ts;
    this.hostName = SingerUtils.getHostname();
    this.jvmUptime = ManagementFactory.getRuntimeMXBean().getUptime();

    StatsSummary summary = Stats.get();
    Map<String, Object> counters = summary.counters();
    this.numExceptions = getCounterValue(counters, SingerMetrics.PROCESSOR_EXCEPTION);
    Map<String, Object> gauges = summary.gauges();
    Double numLogStreams = getGaugeValue(gauges, SingerMetrics.NUM_LOGSTREAMS);
    this.numLogStreams = numLogStreams.longValue();
    Double numStuckLogStreams = getGaugeValue(gauges, SingerMetrics.NUM_STUCK_LOGSTREAMS);
    this.numStuckLogStreams = numStuckLogStreams.longValue();
    this.currentLatency = getGaugeValue(gauges, SingerMetrics.CURRENT_PROCESSOR_LATENCY);

    Iterator<Tuple2<String, Object>> countersIterator = counters.iterator();
    while (countersIterator.hasNext()) {
      Tuple2<String, Object> tuple2 = countersIterator.next();
      String counterName = tuple2._1();
      if (counterName.startsWith(SingerMetrics.NUM_KAFKA_MESSAGES)) {
        String topicName = getOstrichMetricTag(counterName);
        kafkaWrites.put(topicName, Long.toString((Long) tuple2._2()));
      } else if (counterName.startsWith(SingerMetrics.SKIPPED_BYTES)) {
        String streamName = getOstrichMetricTag(counterName);
        skippedBytes.put(streamName, Long.toString((Long) tuple2._2()));
      }
    }

    Map<String, Distribution> metrics = summary.metrics();
    Iterator<Tuple2<String, Distribution>> metricsIterator = metrics.iterator();
    while (metricsIterator.hasNext()) {
      Tuple2<String, Distribution> tuple2 = metricsIterator.next();
      String metricName = tuple2._1();
      if (metricName.startsWith(SingerMetrics.PROCESSOR_LATENCY)) {
        String streamName = getOstrichMetricTag(metricName);
        // TODO: need to change to json format
        latency.put(streamName, tuple2._2().toString());
      }
    }

  }

  /**
   * Constructs a SingerStatus object from the json object written out by #.toString()
   * @param json A JSON string representing a SingerStatus, meaning it has the following keys:
   *         * "version": The Singer version
   *         * "hostname": The host name of the host sending the message
   *         * "jvmuptime": The JVM uptime on that host
   *         * "timestamp": The timestamp at which the message was constructed
   *         * "processorExceptions": The number of processor exceptions on that host
   *         * "numLogStreams": The number of log streams on that host
   *         * "numStuckLogStreams": The number of log streams stuck on that host
   *         * "kafkaWrites": A map of {topic -> number of messages successfully written to Kafka}
   *         * "latency": A map of {topic -> latency in uploading for that topic}
   *         * "skippedBytes": A map of {topic -> skipped bytes in uploading for that topic}
   */
  public SingerStatus(String json) {
    this(getMapFromJson(json));
  }

  @VisibleForTesting
  public SingerStatus(java.util.Map<String, String> kvs) {
    this.version = kvs.get(VERSION_KEY);
    this.hostName = kvs.get(HOSTNAME_KEY);

    // For the following fields, we attempt to extract the long value from the heartbeat message
    // If the field does not exist (e.g. the message is being sent from an old version of singer)
    // we set the value to 0 and log an error.
    long tmpTimestamp = 0L;
    try {
      String timestampStr = kvs.get(TIMESTAMP_KEY);
      if (timestampStr != null) {
        tmpTimestamp = Long.parseLong(timestampStr);
      }
    } catch (NumberFormatException e) {
      LOG.error("Invalid field for timestamp : {}", tmpTimestamp, e);
    }
    this.timestamp = tmpTimestamp;

    long tmpUptime = 0L;
    try {
      tmpUptime = Long.parseLong(kvs.get(JVM_UPTIME_KEY));
    } catch (NumberFormatException e) {
      if (kvs.containsKey(JVM_UPTIME_KEY)) {
        LOG.error("Invalid field for JVM uptime", e);
      }
    }
    this.jvmUptime = tmpUptime;

    long tmpExceptions = 0L;
    try {
      tmpExceptions = Long.parseLong(kvs.get(PROCESSOR_EXCEPTIONS_KEY));
    } catch (NumberFormatException e) {
      // Only log the error if it is an invalid value, as it is expected that old messages may be
      // missing these fields.
      if (kvs.containsKey(PROCESSOR_EXCEPTIONS_KEY)) {
        LOG.error("Invalid field for Processor Exceptions", e);
      }
    }
    this.numExceptions = tmpExceptions;

    long tmpLogStreams = 0L;
    try {
      tmpLogStreams = Long.parseLong(kvs.get(NUM_LOG_STREAMS_KEY));
    } catch (NumberFormatException e) {
      // Only log the error if it is an invalid value, as it is expected that old messages may be
      // missing these fields.
      if (kvs.containsKey(NUM_LOG_STREAMS_KEY)) {
        LOG.error("Invalid field for number of log streams", e);
      }
    }
    this.numLogStreams = tmpLogStreams;

    long tmpStuckStreams = 0L;
    try {
      tmpStuckStreams = Long.parseLong(kvs.get(NUM_STUCK_LOG_STREAMS_KEY));
    } catch (NumberFormatException e) {
      // Only log the error if it is an invalid value, as it is expected that old messages may be
      // missing these fields.
      if (kvs.containsKey(NUM_STUCK_LOG_STREAMS_KEY)) {
        LOG.error("Invalid field for number of stuck streams", e);
      }
    }
    this.numStuckLogStreams = tmpStuckStreams;

    if (kvs.containsKey(KAFKA_WRITES_KEY)) {
      this.kafkaWrites.putAll(getMapFromJson(kvs.get(KAFKA_WRITES_KEY)));
    }

    this.currentLatency
        = kvs.containsKey(CURRENT_LATENCY_KEY) ? Double.parseDouble(kvs.get(CURRENT_LATENCY_KEY))
                                               : 0;

    if (kvs.containsKey(LATENCY_KEY)) {
      this.latency.putAll(getMapFromJson(kvs.get(LATENCY_KEY)));
    }
  }

  private static TreeMap<String, String> getMapFromJson(String message) {
    Gson gson = new Gson();
    return gson.fromJson(message, TreeMap.class);
  }

  private Double getGaugeValue(Map<String, Object> gauges, String gaugeName) {
    Double result = 0.0;
    if (gauges.contains(gaugeName)) {
      Option option = gauges.get(gaugeName);
      result = option.isDefined() ? (Double) option.get() : 0.0;
    }
    return result;
  }

  private Long getCounterValue(Map<String, Object> counters, String counterName) {
    Long result = 0L;
    if (counters.contains(counterName)) {
      Option option = counters.get(counterName);
      result = option.isDefined() ? (Long) option.get() : 0L;
    }
    return result;
  }

  /**
   * In ostrich, the counter name is like
   *  singer.writer.num_kafka_messages_delivery_success topic\u003dauth_log host\u003ddev-yuyang
   *
   * This method is to extract the metric tag like 'auth_log' out of the metric name
   */
  private String getOstrichMetricTag(String metricName) {
    String result = "";
    try {
      String[] strs = metricName.split(" ");
      result = strs[1].split("\u003d")[1];
    } catch (Exception e) {
      LOG.error("Invalid metric name: {}", metricName);
    }
    return result;
  }

  @Override
  public String toString() {
    TreeMap<String, String> kvs = new TreeMap<>();
    kvs.put(VERSION_KEY, this.version);
    kvs.put(HOSTNAME_KEY, this.hostName);
    kvs.put(JVM_UPTIME_KEY, Long.toString(this.jvmUptime));
    kvs.put(TIMESTAMP_KEY, Long.toString(this.timestamp));
    kvs.put(PROCESSOR_EXCEPTIONS_KEY, Long.toString(this.numExceptions));
    kvs.put(NUM_LOG_STREAMS_KEY, Long.toString(this.numLogStreams));
    kvs.put(NUM_STUCK_LOG_STREAMS_KEY, Long.toString(this.numStuckLogStreams));

    Gson gson = new Gson();
    kvs.put(KAFKA_WRITES_KEY, gson.toJson(this.kafkaWrites));
    kvs.put(CURRENT_LATENCY_KEY, Double.toString(this.currentLatency));
    kvs.put(LATENCY_KEY, gson.toJson(this.latency));
    kvs.put(SKIPPED_BYTES_KEY, gson.toJson(this.skippedBytes));
    String json = gson.toJson(kvs);
    return json;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof SingerStatus) {
      SingerStatus that = (SingerStatus) o;
      return ((this.version == null ? that.version == null : this.version.equals(that.version)) &&
                  (this.hostName == null ? that.hostName == null
                                         : this.hostName.equals(that.hostName)) &&
                  (this.timestamp == that.timestamp) &&
                  (this.jvmUptime == that.jvmUptime) &&
                  (this.numExceptions == that.numExceptions) &&
                  (this.numLogStreams == that.numLogStreams) &&
                  (this.numStuckLogStreams == that.numStuckLogStreams) &&
                  (this.kafkaWrites == null ? that.kafkaWrites == null
                                            : this.kafkaWrites.equals(that.kafkaWrites)) &&
                  (this.latency == null ? that.latency == null
                                        : this.latency.equals(that.latency)) &&
                  (this.skippedBytes == null ? that.skippedBytes == null
                                             : this.skippedBytes.equals(that.skippedBytes))
      );
    }
    return false;
  }

  public String getVersion() {
    return version;
  }

  public String getHostName() {
    return hostName;
  }

  public long getJvmUptime() {
    return jvmUptime;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getNumExceptions() {
    return numExceptions;
  }

  public long getNumLogStreams() {
    return numLogStreams;
  }

  public long getNumStuckLogStreams() {
    return numStuckLogStreams;
  }

  public TreeMap<String, String> getKafkaWrites() {
    return kafkaWrites;
  }

  public TreeMap<String, String> getLatency() {
    return latency;
  }

  public TreeMap<String, String> getSkippedBytes() {
    return skippedBytes;
  }
}
