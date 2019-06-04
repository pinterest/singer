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

import com.pinterest.singer.reader.ThriftReader;
import com.pinterest.singer.thrift.LogMessage;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.io.PrintStream;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class ThriftLogDumper {

  /**
   * This file is a helper tool for dumping thrift logs in to human readable format.
   * Example:
   * java -cp <your classpath>    \ # Class path to dependencies and singer jar,
   *                              \ # e.g. `target/*:target/lib/*`
   * com.pinterest.singer.tools.ThriftLogDumper \
   * -i /some/path/to/the.log     \ # Input log file path, required
   * -o /some/path/to/the.output  \ # Output file path, default: STDOUT
   * -n 1                         \ # Number of messages to print, default: INTMAX
   * --max-message-size 32768     \ # Max size of the message, default: 16384(16KB)
   * --offset 1024                \ # Offset from beginning of the file to read, default: 0
   * --no-timestamp               \ # Add this flag to output message body only
   * --json                       \ # Add this flag to output message in JSON format,
   *                              \ # otherwise output will be in text
   * --pretty                     \ # Prettifies JSON format
   * --thrift-schema name.of.your.thrift.schema.class \
   *                                # Thrift schema class used to deserialize the message.
   *                                # Will cast message body to string if not provided.
   *                                # (The class should be resolvable by the class paths)
   *
   * Sample Output (Text Format):
   * [2019-01-01T12:34:56.123456789]{...message body...}
   *
   * Sample Output (JSON Format, raw):
   * {"timestampInNanos":123456,"message":"{...raw message body...}"}
   *
   * Sample Output (JSON Format, with Thrift schema provided):
   * {"timestampInNanos":123456,"deserializedMessage":{...message body...}}
   */

  private static final int DEFAULT_READ_BUF_SIZE = 16384;

  private static void printUsageAndExit() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("ThriftLogDumper", buildOptions());
    System.exit(1);
  }

  private static Options buildOptions() {
    Option input = new Option("i", "input", true, "Input thrift logger log file");
    input.setRequired(true);

    Option output = new Option("o", "output", true, "Output file. Default: STDOUT");
    Option maxMessageSize =
        new Option(null, "max-message-size", true,
            "Max thrift message size allowed. Default: 16384");
    Option startOffset = new Option(null, "offset", true,
        "Byte offset from the beginning of the file to start reading. Default: 0");
    Option numOfMessages =
        new Option("n", "number-of-messages", true, "Number of messages to dump. Default: INTMAX");
    Option noTimestamp = new Option(null, "no-timestamp", false, "Output message only");
    Option toJson = new Option(null, "json", false, "Output as JSON format");
    Option pretty = new Option(null, "pretty", false, "Prettifies the JSON formatted messages");
    Option thriftSchema =
        new Option(null, "thrift-schema", true,
            "Thrift schema class used for deserialization. Default: cast message body to String.");

    Options options = new Options();
    options
        .addOption(input)
        .addOption(output)
        .addOption(maxMessageSize)
        .addOption(startOffset)
        .addOption(numOfMessages)
        .addOption(noTimestamp)
        .addOption(toJson)
        .addOption(pretty)
        .addOption(thriftSchema);
    return options;
  }

  private static CommandLine parseCommandLine(String[] args) {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(buildOptions(), args);
    } catch (ParseException | NumberFormatException e) {
      e.printStackTrace();
      printUsageAndExit();
    }
    return cmd;
  }


  public static final void main(String[] args) throws Exception {
    CommandLine cmd = parseCommandLine(args);
    // Default to 16k
    int maxMessageSize = 16384;
    long startByteOffset = 0;
    // Default to maximum value.
    int numOfMessages = Integer.MAX_VALUE;
    boolean noTimestamp = false;
    DumperType dumperType = DumperType.TEXT;
    boolean pretty = false;
    String thriftSchema = null;

    try {
      if (cmd.hasOption("max-message-size")) {
        maxMessageSize = Integer.parseInt(cmd.getOptionValue("max-message-size"));
      }

      if (cmd.hasOption("offset")) {
        startByteOffset = Long.parseLong(cmd.getOptionValue("offset"));
      }

      if (cmd.hasOption("n")) {
        numOfMessages = Integer.parseInt(cmd.getOptionValue("n"));
      }
      if (cmd.hasOption("no-timestamp")) {
        noTimestamp = true;
      }
      if (cmd.hasOption("json")) {
        dumperType = DumperType.JSON;
      }
      if (cmd.hasOption("pretty")) {
        pretty = true;
      }
      if (cmd.hasOption("thrift-schema")) {
        thriftSchema = cmd.getOptionValue("thrift-schema");
      }
    } catch (NumberFormatException e) {
      e.printStackTrace();
      printUsageAndExit();
    }

    String input = cmd.getOptionValue("input");
    String output = cmd.getOptionValue("output");
    Dumper dumper = null;
    switch (dumperType) {
      case JSON:
        dumper = new JsonDumper(pretty);
        break;
      case TEXT:
      default:
        dumper = new TextDumper();
    }
    PrintStream outputPrintStream = System.out;

    if (output != null && !output.isEmpty()) {
      outputPrintStream = new PrintStream(output);
    }

    try (ThriftReader<LogMessage> thriftReader = new ThriftReader<LogMessage>(input,
        new LogMessageFactory(), new BinaryProtocolFactory(), DEFAULT_READ_BUF_SIZE,
        maxMessageSize)) {
      thriftReader.setByteOffset(startByteOffset);
      LogMessage logMessage;

      for (int i = 0; i < numOfMessages; i++) {
        logMessage = thriftReader.read();
        if (logMessage == null) {
          break;
        } else {
          outputPrintStream.println(dumper.dump(logMessage, noTimestamp, thriftSchema));
        }
      }
    }
  }

  private interface Dumper {

    String dump(LogMessage logMessage, boolean noTimestamp, String thriftSchema) throws Exception;
  }

  private enum DumperType {
    TEXT,
    JSON,
  }

  // Factory that create LogMessage thrift objects.
  private static final class LogMessageFactory
      implements ThriftReader.TBaseFactory<com.pinterest.singer.thrift.LogMessage> {

    public LogMessage get() {
      return new LogMessage();
    }
  }

  private static final class BinaryProtocolFactory implements ThriftReader.TProtocolFactory {

    public TProtocol get(TTransport transport) {
      return new TBinaryProtocol(transport);
    }
  }

  /**
   * Thrift log dumper that dumps the thrift log message into `[timestamp]message` format.
   */
  private static class TextDumper implements Dumper {

    @Override
    public String dump(LogMessage logMessage, boolean noTimestamp, String thriftSchema)
        throws Exception {
      long timeInNanos = logMessage.getTimestampInNanos();
      LocalDateTime logDatetime =
          LocalDateTime.ofEpochSecond(timeInNanos / 1000000000L, (int) (timeInNanos % 1000000000),
              ZoneOffset.of("Z"));

      StringBuilder stringBuilder = new StringBuilder();

      if (!noTimestamp) {
        stringBuilder.append("[" + logDatetime.toString() + "]");
      }

      if (thriftSchema != null && !thriftSchema.isEmpty()) {
        TBase tObj = deserializeBytesToThriftObj(logMessage.getMessage(), thriftSchema);
        stringBuilder.append(tObj.toString());
      } else {
        stringBuilder.append(new String(logMessage.getMessage()));
      }
      return stringBuilder.toString();
    }
  }

  /*
   *  JsonDumper provides a Dumper that dumps the LogMessages into JSON format
   */
  private static class JsonDumper implements Dumper {

    private Gson gson;

    public JsonDumper() {
      gson = new Gson();
    }

    public JsonDumper(boolean pretty) {
      if (pretty) {
        gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
      } else {
        gson = new Gson();
      }
    }

    /**
     * JsonDumperMessage is a helper class for us to encode the message in Gson
     * only one of the deserializedMessage and message fields should be set
     *
     */
    private class JsonDumperMessage {

      Long timeStampInNanos;
      Object deserializedMessage;
      String message;
    }

    @Override
    public String dump(LogMessage logMessage, boolean noTimestamp, String thriftSchema)
        throws Exception {
      JsonDumperMessage log = new JsonDumperMessage();

      if (!noTimestamp) {
        log.timeStampInNanos = logMessage.getTimestampInNanos();
      }
      if (thriftSchema != null && !thriftSchema.isEmpty()) {
        TBase tObj = deserializeBytesToThriftObj(logMessage.getMessage(), thriftSchema);
        log.deserializedMessage = tObj;
      } else {
        log.message = new String(logMessage.getMessage());
      }
      return gson.toJson(log);
    }
  }

  private static TBase deserializeBytesToThriftObj(byte[] bytes, String thriftSchema)
      throws Exception {
    TBase obj = (TBase) Class.forName(thriftSchema).getConstructor().newInstance();
    TDeserializer deserializer = new TDeserializer();
    deserializer.deserialize(obj, bytes);
    return obj;
  }
}