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
package com.pinterest.singer.e2e;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.pinterest.singer.utils.SingerTestHelper;
import com.pinterest.singer.utils.SingerUtils;

import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

/**
 *  End to end test for measuring the latency between logging to disk and reading out of Kafka
 */
public class SingerLatencyTest {

  private static final String singerTestBaseDir = "/tmp/singer_latency_test";
  private static final String singerBinaryDir = singerTestBaseDir + "/singer";
  private static final String singerDataDir = singerTestBaseDir + "/data";
  private static final String singerConfigDir = singerTestBaseDir + "/config";
  private static final String singerConfigConfDir = singerConfigDir + "/conf.d";

  private static final String testTopic = "singer_latency";
  private static final String auditTopic = "singer_test_event_audit";

  private static int rotateThresholdKilobytes = 200;
  private static int numTestMessages = 2000;
  private static int batchSize = 400;
  private static int numExpectedMessages = 2000;

  private final static int NUM_TEST_PARTITIONS = 1;

  public static void main(String[] args) throws Exception {
    SingerLatencyTest test = new SingerLatencyTest();
    test.testLoggingLatency();
  }


  public void testLoggingLatency() throws Exception {
    System.out.println("Testing Singer logging latency: ");

     // initialize the test environment
    System.out.println("Initialize the test environment");
    SingerTestHelper.e2eTestCleanup(singerTestBaseDir, true);

    SingerTestHelper.createSingerConfig(
        singerConfigDir,
        singerConfigConfDir,
        singerDataDir,
        "singer_latency",
        batchSize,
        testTopic,
        auditTopic,
        60,
        "singer_heartbeat");

    // launch singer
    Process singerProc = SingerTestHelper
        .startSingerProcess(singerBinaryDir, singerConfigDir, SingerEndToEndTest.class);


    LogWriter logWriter = new LogWriter("singer_latency",
        singerDataDir, rotateThresholdKilobytes, numTestMessages);
    Thread logWriterThread = new Thread(logWriter);
    logWriterThread.start();


    SingerOutputRetriever outputRetriever = new SingerOutputRetriever(singerProc.getErrorStream());
    Thread outThread = new Thread(outputRetriever);
    outThread.start();

    // read messages from kafka
    Properties properties = SingerTestHelper.createKafkaConsumerConfig();
    KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer(properties);
    kafkaConsumer.subscribe(Arrays.asList(testTopic));

    List<Long> latencyCounts = new ArrayList<>();

    String hostName = SingerUtils.getHostname();
    System.out.println("Fetching messages from " + hostName + " : ");
    int numReceivedMessages = 0;
    while (numReceivedMessages < numExpectedMessages) {

      ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(500L));
      for (ConsumerRecord<byte[], byte[]> record : records) {
        try {
          byte[] bytes = record.value();
          JsonParser jsonParser = new JsonParser();
          JsonObject jsonObject = (JsonObject)jsonParser.parse(new String(bytes));
          long eventTimestamp = jsonObject.get("timestamp").getAsLong();
          long now = System.currentTimeMillis();
          latencyCounts.add(now - eventTimestamp);
          System.out.println(" delay in millis: " + (now - eventTimestamp));
        } catch (Exception e) {
          System.out.println("Found an exception!!!");
        }
        numReceivedMessages++;
      }
      System.out.println("Reading one kafka message : " + numReceivedMessages);
    }

    latencyCounts = new ArrayList<>(latencyCounts.subList(400, latencyCounts.size()));
    latencyCounts.sort(Comparator.<Long>naturalOrder());

    int p50Index = latencyCounts.size() / 2;
    int p90Index = latencyCounts.size() * 9 /10;
    int p99Index = latencyCounts.size() * 99 / 100;

    System.out.println("min : " + latencyCounts.get(0));
    System.out.println("P50 : " + latencyCounts.get(p50Index));
    System.out.println("P90 : " + latencyCounts.get(p90Index));
    System.out.println("P99 : " + latencyCounts.get(p99Index));
    System.out.println("max : " + latencyCounts.get(latencyCounts.size() - 1));

    logWriterThread.join();
    kafkaConsumer.close();
    singerProc.destroy();
    SingerTestHelper.e2eTestCleanup(singerTestBaseDir, false);
  }
}

