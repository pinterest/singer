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

import com.pinterest.singer.common.SingerStatus;
import com.pinterest.singer.utils.SingerTestHelper;
import com.pinterest.singer.utils.SingerUtils;
import com.pinterest.singer.client.ThriftLoggerFactory;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

public class SingerHeartbeatTest {

  private static final String singerTestBaseDir = "/tmp/singer_e2e_test";
  private static final String singerBinaryDir = singerTestBaseDir + "/singer";
  private static final String singerDataDir = singerTestBaseDir + "/data";
  private static final String singerConfigDir = singerTestBaseDir + "/config";
  private static final String singerConfigConfDir = singerConfigDir + "/conf.d";

  private static final String heartbeatTopic = "singer_heartbeat";

  private static int heartbeatIntervalInMilliSeconds;
  private static int numHeartbeats;

  private static void printUsageAndExit() {
    System.err.println("USAGE: java " + SingerEndToEndTest.class.getName()
        + " heartbeatIntervalInSeconds  numHeartbeats");
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      printUsageAndExit();
    }

    try {
      heartbeatIntervalInMilliSeconds = Integer.parseInt(args[0]) * 1000;
      numHeartbeats = Integer.parseInt(args[1]);

      System.out.println(
          "heartbeatIntervalsInSeconds = " + heartbeatIntervalInMilliSeconds + "  numHeartbeats = "
              + numHeartbeats);
    } catch (Exception e) {
      printUsageAndExit();
    }

    SingerTestHelper.e2eTestCleanup(singerTestBaseDir, true);
    SingerHeartbeatTest test = new SingerHeartbeatTest();
    test.testHeartBeat();
    SingerTestHelper.e2eTestCleanup(singerTestBaseDir, false);
  }


  void testHeartBeat() {
    int numReceivedHeartbeats = 0;

    SingerTestHelper.createSingerConfig(
        singerConfigDir,
        singerConfigConfDir,
        singerDataDir,
        "singer_test_event",
        100,
        "singer_test_event",
        "",
        heartbeatIntervalInMilliSeconds / 1000,
        heartbeatTopic);

    SingerTestHelper.createSingerConfigLConfFile(
        singerConfigConfDir,
        "singer.test2.properties",
        singerDataDir,
        "singer_test_event_2",
        100,
        "singer_test_event",
        "");

    Process
        singerProc =
        SingerTestHelper
            .startSingerProcess(singerBinaryDir, singerConfigDir, SingerHeartbeatTest.class);

    File outputDir = new File(singerDataDir);
    ThriftLoggerFactory.initialize(outputDir, 100);

    SingerTestHelper.createLogStream(singerDataDir, "singer_test_event", 100, 500);
    SingerTestHelper.createLogStream(singerDataDir, "singer_test_event_2", 100, 500);

    SingerOutputRetriever outputRetriever = new SingerOutputRetriever(singerProc.getErrorStream());
    Thread outThread = new Thread(outputRetriever);
    outThread.start();

    try {
      Thread.sleep(20 * 1000);

      Properties properties = SingerTestHelper.createKafkaConsumerConfig();
      KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer(properties);
      kafkaConsumer.subscribe(Arrays.asList(heartbeatTopic));

      SingerStatus status = null;

      for (int i = 0; i < numHeartbeats; i++) {
        Thread.sleep(heartbeatIntervalInMilliSeconds);

        String hostName = SingerUtils.getHostname();
        System.out.println("Fetching heartbeat messages from " + hostName + " : ");

        ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(500L);
        for (ConsumerRecord<byte[], byte[]> record : records) {
          String msg = new String(record.value());
          status = new Gson().fromJson(msg, SingerStatus.class);

          if (System.currentTimeMillis() - status.getTimestamp() > heartbeatIntervalInMilliSeconds
                  || !status.hostName.equals(hostName)) {
            System.out.println(msg);
            status = new Gson().fromJson(msg, SingerStatus.class);
            kafkaConsumer.commitSync();
          }

          System.out.println(msg);
          kafkaConsumer.commitSync();
          numReceivedHeartbeats++;
          assert (msg.contains("data.test"));
          assert (msg.contains("singer.test2"));
        }
      }
      kafkaConsumer.close();
    } catch (Exception e) {
      e.printStackTrace();
      assert (false);
    } finally {
      singerProc.destroy();
    }

    assert (numReceivedHeartbeats == numHeartbeats);
  }
}
