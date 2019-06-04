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

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;

/**
 * A client library for sending metrics to an OpenTSDB server.
 *
 * The API for sending data to OpenTSDB is documented here:
 *
 *  http://opentsdb.net/docs/build/html/user_guide/writing.html
 *
 * Example:
 *
 *  <code>
 *   OpenTSDBClient client = new OpenTSDBClient("localhost", 4242);
 *   MetricsBuffer buffer = new MetricsBuffer();
 *
 *   buffer.addMetric("foo.bar.sprockets", System.currentTimeMillis() / 1000L, 1.0, "host=x.y.z");
 *   client.sendMetrics(buffer);
 *
 *   buffer.reset()
 *   buffer.addMetric("a.b.c", System.getCurrentTimeMillis() / 1000L, 3.5, "tag=value", "x=y");
 *   buffer.addMetric("x.y.z", System.getCurrentTimeMillis() / 1000L, 5.3, "a=b", "q=r");
 *   client.sendMetrics(buffer);
 *  </code>
 */
public class OpenTsdbClient {
  private static final Logger LOG = LoggerFactory.getLogger(OpenTsdbClient.class);
  private static final int CONNECT_TIMEOUT_MS = 100;
  private final SocketAddress address;

  public static final class MetricsBuffer {

    public MetricsBuffer() {
      this.buffer = new StringBuilder();
    }

    /**
     * Add a single metric to the buffer.
     *
     * @param name the name of the metric, like "foo.bar.sprockets".
     * @param epochSecs the UNIX epoch time in seconds.
     * @param value the value of the metric at this epoch time.
     * @param tags a list of one or more tags, each of which must be formatted as "name=value".
     */
    public void addMetric(String name, int epochSecs, float value, String... tags) {
      addMetric(name, epochSecs, value, SPACE_JOINER.join(tags));
    }

    public void addMetric(String name, int epochSecs, float value, String tags) {
      buffer.append("put ")
          .append(name)
          .append(" ")
          .append(epochSecs)
          .append(" ")
          .append(value)
          .append(" ")
          .append(tags)
          .append("\n");
    }

    /**
     * Reset the metrics buffer for reuse, this discards all previous data.
     */
    public void reset() {
      buffer.setLength(0);
    }

    @Override
    public String toString() {
      return buffer.toString();
    }

    private final StringBuilder buffer;
    private static final Joiner SPACE_JOINER = Joiner.on(" ");
  }

  public static class OpenTsdbClientException extends Exception {

    public OpenTsdbClientException(Throwable causedBy) {
      super(causedBy);
    }
  }

  public static final class ConnectionFailedException extends OpenTsdbClientException {

    public ConnectionFailedException(Throwable causedBy) {
      super(causedBy);
    }
  }

  public static final class SendFailedException extends OpenTsdbClientException {

    public SendFailedException(Throwable causedBy) {
      super(causedBy);
    }
  }

  public OpenTsdbClient(String host, int port) throws UnknownHostException {
    InetAddress address = InetAddress.getByName(host);
    this.address = new InetSocketAddress(address, port);
  }

  public void sendMetrics(MetricsBuffer buffer)
      throws ConnectionFailedException, SendFailedException {
    Socket socket = null;
    try {
      try {
        socket = new Socket();
        socket.connect(address, CONNECT_TIMEOUT_MS);
      } catch (IOException ioex) {
        throw new ConnectionFailedException(ioex);
      }

      try {
        // There is no way to set a time out for blocking send calls. Thanks Java!
        socket.getOutputStream().write(buffer.toString().getBytes());
      } catch (IOException ioex) {
        throw new SendFailedException(ioex);
      }
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException ioex) {
          LOG.warn("Failed to close socket to OpenTSDB", ioex);
        }
      }
    }
  }

}
