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
package com.pinterest.singer.tools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.pinterest.singer.monitor.LogStreamManager;

/**
 * Allows inspection of Singer data structures without needing to attach a
 * debugger
 */
public class SingerCommandServer implements Runnable {

    public static void main(String[] args) throws IOException, InterruptedException {
        Thread th = new Thread(new SingerCommandServer());
        th.start();
        th.join();
    }

    @Override
    public void run() {
        try {
            File file = new File("/tmp/cmd.txt");
            file.createNewFile();
            long modified = 0;
            while (true) {
                long tModified = file.lastModified();
                if (tModified > modified) {
                    byte[] bytes = Files.readAllBytes(file.toPath());
                    String x = new String(bytes).trim();
                    if (!x.isEmpty()) {
                        processCommand(x);
                    }
                    modified = tModified;
                }
                Thread.sleep(2000);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void processCommand(String cmd) {
        switch (cmd) {
        case "dump_logstreams":
            System.out.println("Dumping all logstreams:" + LogStreamManager.getLogStreams());
            break;
        default:
            System.err.println("Unknown cmd:" + cmd);
            break;
        }
    }
}
