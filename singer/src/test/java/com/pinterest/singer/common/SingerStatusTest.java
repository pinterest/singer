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

import com.google.gson.Gson;
import org.junit.Test;

import java.util.Collections;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public class SingerStatusTest {
    @Test
    public void defaultStatusTest() throws Exception {
        SingerStatus status = new SingerStatus(new Random().nextLong());
        SingerStatus status2 = new SingerStatus(status.toString());
        assertEquals(status, status2);
    }

    @Test
    public void oldStatusTest() {
        TreeMap<String, String> kvs = new TreeMap<>();
        String version = "0.5.19";
        String hostName = "test-host-name";
        long uptime = new Random().nextLong();
        kvs.put("version", version);
        kvs.put("hostname", hostName);
        kvs.put("jvmuptime", Long.toString(uptime));
        kvs.put("currentLatency", Double.toString(0));
        Gson gson = new Gson();
        String json = gson.toJson(kvs);

        SingerStatus status = new SingerStatus(json);
        assertEquals("Version should be deserialized", version, status.getVersion());
        assertEquals("Host name should be deserialized", hostName, status.getHostName());
        assertEquals("Uptime should be deserialized", uptime, status.getJvmUptime());

        // Everything else should be null / empty / 0
        assertEquals("no exceptions", 0, status.getNumExceptions());
        assertEquals("no log streams", 0, status.getNumLogStreams());
        assertEquals("no stuck log streams", 0, status.getNumStuckLogStreams());
        assertEquals("no kafka writes", Collections.EMPTY_MAP, status.getKafkaWrites());
        assertEquals("no latency", Collections.EMPTY_MAP, status.getLatency());
        assertEquals("no skippedBytes", Collections.EMPTY_MAP, status.getSkippedBytes());
    }
}
