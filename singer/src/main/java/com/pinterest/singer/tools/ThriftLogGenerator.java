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

import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.thrift.ThriftMessage;
import com.pinterest.singer.utils.SimpleThriftLogger;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Thrift log generator that generate multiple logical log sequences. All messages in one
 * logical sequences has the same key and has sequential sequence number starting from 0.
 *
 * The key is the logical sequence id where sequence numbers start. The message contains
 * specified number of random bytes.
 *
 * This tool is used for load/test test. It can be used to generate logs at specified rate.
 */
public class ThriftLogGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(ThriftLogGenerator.class);

  private final byte[] logGeneratorId;
  private final String logDir;
  private final String logFileName;

  private final RateLimiter rateLimiter;
  private final SimpleThriftLogger<LogMessage> logger;
  private final Random random;

  private final int maxNumOfMessages;
  private final int numOfBytesPerLogFile;
  private final int numOfLogFiles;
  private final int payloadSizeInBytes;

  // Num of log message sequences that this thrift log generator will generate. Each log message
  // sequence consists of a sequence of log messages with the same key and continuous
  // numbered from 0.
  private final int numOfLogMessageSequence;
  // Map from sequence index to the next log message number in the sequence.
  private final List<Long> nextSequenceNums;
  // Map from sequence index to sequence id
  private final List<byte[]> sequenceIds;

  private final TSerializer serializer;

  public ThriftLogGenerator(
      String logDir,
      String logFileName,
      int bytesPerSecond,
      int maxNumOfMessages,
      int numOfBytesPerLogFile,
      int numOfLogFiles,
      int payloadSizeInBytes) throws Exception {
    UUID uuid = UUID.randomUUID();
    this.logGeneratorId = ByteBuffer.wrap(new byte[16])
        .putLong(uuid.getMostSignificantBits())
        .putLong(uuid.getLeastSignificantBits())
        .array();
    this.logDir = logDir;
    this.logFileName = logFileName;

    this.rateLimiter = RateLimiter.create(bytesPerSecond);
    this.logger = new SimpleThriftLogger<>(FilenameUtils.concat(logDir, logFileName));
    this.random = new Random();

    this.maxNumOfMessages = maxNumOfMessages;
    this.numOfBytesPerLogFile = numOfBytesPerLogFile;
    this.numOfLogFiles = numOfLogFiles;
    this.payloadSizeInBytes = payloadSizeInBytes;

    this.numOfLogMessageSequence = 100;
    this.nextSequenceNums = Lists.newArrayListWithExpectedSize(numOfLogMessageSequence);
    this.sequenceIds = Lists.newArrayListWithExpectedSize(numOfLogMessageSequence);

    this.serializer = new TSerializer(new TBinaryProtocol.Factory());
  }

  public void generateLogs() throws Exception {
    // Initialize the nextSequenceNums and sequenceIds.
    for (int i = 0; i < numOfLogMessageSequence; ++i) {
      nextSequenceNums.add(0L);
      UUID uuid = UUID.randomUUID();
      ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
      buffer.putLong(uuid.getMostSignificantBits());
      buffer.putLong(uuid.getLeastSignificantBits());
      sequenceIds.add(buffer.array());
    }

    for (int i = 1; maxNumOfMessages < 0 || i <= maxNumOfMessages; ++i) {
      LogMessage message = getLogMessage();
      long byteOffset = logger.getByteOffset();
      logger.logThrift(message);
      long bytesWritten = logger.getByteOffset() - byteOffset;
      rateLimiter.acquire((int) bytesWritten);
      if (logger.getByteOffset() > numOfBytesPerLogFile) {
        logger.rotate();
        LOG.info("Rotated log file.\n");
        removeLogFileIfNeeded(numOfLogFiles);
      }
    }
  }

  public static final void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("logDir")
        .hasArg()
        .isRequired()
        .withDescription("Log dir")
        .create("logDir"));
    options.addOption(OptionBuilder.withArgName("logFileName")
        .hasArg()
        .isRequired()
        .withDescription("Log file name")
        .create("logFileName"));
    options.addOption(OptionBuilder.withArgName("maxNumOfMessages")
        .hasArg()
        .isRequired()
        .withDescription("Max number of messages; negative number means infinite.")
        .create("maxNumOfMessages"));
    options.addOption(OptionBuilder.withArgName("numOfBytesPerLogFile")
        .hasArg()
        .isRequired()
        .withDescription("Num of bytes per log file")
        .create("numOfBytesPerLogFile"));
    options.addOption(OptionBuilder.withArgName("numOfLogFiles")
        .hasArg()
        .isRequired()
        .withDescription("Number of log files")
        .create("numOfLogFiles"));
    options.addOption(OptionBuilder.withArgName("bytesPerSecond")
        .hasArg()
        .isRequired()
        .withDescription("Bytes generated per second")
        .create("bytesPerSecond"));
    options.addOption(OptionBuilder.withArgName("payloadSizeInBytes")
        .hasArg()
        .isRequired()
        .withDescription("Message size in bytes")
        .create("payloadSizeInBytes"));

    CommandLineParser parser = new GnuParser();
    try {
      CommandLine cmd = parser.parse(options, args);
      String logDir = cmd.getOptionValue("logDir");
      String logFileName = cmd.getOptionValue("logFileName");
      ThriftLogGenerator generator = new ThriftLogGenerator(
          logDir,
          logFileName,
          Integer.parseInt(cmd.getOptionValue("bytesPerSecond")),
          Integer.parseInt(cmd.getOptionValue("maxNumOfMessages")),
          Integer.parseInt(cmd.getOptionValue("numOfBytesPerLogFile")),
          Integer.parseInt(cmd.getOptionValue("numOfLogFiles")),
          Integer.parseInt(cmd.getOptionValue("payloadSizeInBytes")));
      LOG.info("Generating log stream: {} into: {}", logFileName, logDir);
      generator.generateLogs();
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("ThriftLogGenerator", options);
    }
  }

  private LogMessage getLogMessage() throws TException {
    // Choose a sequence to append this log message onto.
    int sequenceIndex = random.nextInt(numOfLogMessageSequence);

    // Get next sequence number.
    Long nextSequenceNum = nextSequenceNums.get(sequenceIndex);
    if (nextSequenceNum == Long.MAX_VALUE) {
      nextSequenceNum = 0L;
    }

    // Set the message part of log message.
    byte[] payloadBytes = new byte[payloadSizeInBytes];
    random.nextBytes(payloadBytes);
    ThriftMessage thriftMessage = new ThriftMessage(
        nextSequenceNum, ByteBuffer.wrap(payloadBytes));
    byte[] thriftMessageBytes = serializer.serialize(thriftMessage);
    LogMessage logMessage = new LogMessage(ByteBuffer.wrap(thriftMessageBytes));

    // Set the key part of log message to the sequence id so that all log messages in a sequence
    // will have the same key.
    logMessage.setKey(sequenceIds.get(sequenceIndex));

    // Set the message timestamp.
    logMessage.setTimestampInNanos(System.currentTimeMillis() * 1000000);

    // Bump the next sequence number.
    nextSequenceNums.set(sequenceIndex, nextSequenceNum + 1);

    return logMessage;
  }

  private void removeLogFileIfNeeded(int numOfLogFiles) throws IOException {
    File dir = new File(logDir);
    FilenameFilter filter = new PrefixFileFilter(logFileName);
    List<File> files = Arrays.asList(dir.listFiles(filter));
    Collections.sort(files, LastModifiedFileComparator.LASTMODIFIED_COMPARATOR);
    if (files.size() > numOfLogFiles) {
      for (int i = 0; i < files.size() - numOfLogFiles; ++i) {
        FileUtils.forceDelete(files.get(i));
        System.out.print(
            String.format(
                "Removed log file: %s.\n",
                files.get(i).getPath()));
      }
    }
  }
}
