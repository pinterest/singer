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

import com.pinterest.singer.utils.SingerUtils;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


/**
 *  A command line tool for monitoring the file system events of a given directory.
 *  Usage:
 *      java -cp ..  com.pinterest.singer.tools.FileSystemEventReader -path dir_path -polling_interval_ms 500
 */
public class FileSystemEventReader {

  private static final String PATH = "path";
  private static final String POLL_INTERVAL_MS = "polling_interval_ms";
  private static final Options options = new Options();

  public static void readEvents(String pathStr, long pollIntervalMs) throws IOException, InterruptedException {
    WatchService watchService = FileSystems.getDefault().newWatchService();
    Path path = Paths.get(pathStr);
    SingerUtils.registerWatchKey(watchService, path);

    while (true) {
      WatchKey watchKey = watchService.take();
      int numHiddenFileEvents = 0;
      int numNormalFileEvents = 0;
      for (final WatchEvent<?> event : watchKey.pollEvents()) {
        WatchEvent.Kind<?> kind = event.kind();
        Path filePath = (Path) event.context();
        if (filePath.toString().startsWith(".")) {
          numHiddenFileEvents++;
          continue;
        } else {
          numNormalFileEvents++;
          System.out.println("kind = " + kind + ": " + event.context());
        }
      }
      System.out.println("Events in one batch      : " + (numHiddenFileEvents + numNormalFileEvents));
      System.out.println("Events from normal files : " + numNormalFileEvents);
      System.out.println("Events from hidden files : " + numHiddenFileEvents);
      watchKey.reset();
      Thread.sleep(pollIntervalMs);
    }
  }

  private static CommandLine parseCommandLine(String[] args) {
    Option path = new Option(PATH, true, "path to be monitored");
    Option poll_interval_ms =
        new Option(POLL_INTERVAL_MS, true, "interval in milliseconds to poll for file system events");
    options.addOption(path).addOption(poll_interval_ms);

    if (args.length < 1) {
      printUsageAndExit();
    }
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException | NumberFormatException e) {
      printUsageAndExit();
    }
    return cmd;
  }

  private static void printUsageAndExit() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("FileSystemEventReader", options);
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    CommandLine commandLine = parseCommandLine(args);
    String path = commandLine.getOptionValue(PATH);
    long pollIntervalMs = Long.parseLong(commandLine.getOptionValue(POLL_INTERVAL_MS, "500"));
    readEvents(path, pollIntervalMs);
  }
}
