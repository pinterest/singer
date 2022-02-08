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
package com.pinterest.singer.admin;

import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.monitor.LogStreamManager;
import com.pinterest.singer.thrift.configuration.AdminConfig;

import com.twitter.ostrich.stats.Stats;
import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import org.newsclub.net.unix.AFUNIXSocketCredentials;
import org.newsclub.net.unix.server.AFUNIXSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class AdminServer extends AFUNIXSocketServer {
  private static final Logger logger = LoggerFactory.getLogger(AdminServer.class.getName());
  private static volatile AdminServer INSTANCE = null;

  private final Set<Long> allowedUids;

  private AdminServer(AdminConfig adminConfig) throws SocketException {
    super(AFUNIXSocketAddress.of(new File(adminConfig.getSocketFile())));
    allowedUids = new HashSet<>(adminConfig.getAllowedUids());
  }

  public static AdminServer getInstance(AdminConfig adminConfig) throws IOException {
    if (INSTANCE == null) {
      synchronized (AdminServer.class) {
        if (INSTANCE == null) {
          INSTANCE = new AdminServer(adminConfig);
        }
      }
    }
    return INSTANCE;
  }

  public void init() throws InterruptedException {
    this.startAndWait(500, TimeUnit.MILLISECONDS);
  }

  @Override
  protected void onServerStarting() {
    logger.info("AdminServer starting...");
    Stats.setGauge(SingerMetrics.ADMIN_SERVER_STARTED, 1);
  }

  @Override
  protected void onListenException(Exception e) {
    logger.warn("Exception when listening", e);
  }

  @Override
  protected void onSocketExceptionDuringAccept(SocketException e) {
    logger.warn("Exception during accept", e);
  }

  @Override
  protected void onSocketExceptionAfterAccept(Socket socket, SocketException e) {
    logger.warn("Exception after accept", e);
  }

  @Override
  protected void onServingException(Socket socket, Exception e) {
    logger.warn("Exception when serving", e);
  }

  @Override
  protected void onServerStopped(ServerSocket socket) {
    Stats.setGauge(SingerMetrics.ADMIN_SERVER_STARTED, 0);
  }

  @Override
  protected void doServeSocket(Socket socket) throws IOException {
    logger.info("Serving socket...");
    if (socket instanceof AFUNIXSocket) {
      doServeSocket((AFUNIXSocket) socket);
    } else {
      logger.error("Invalid socket type: " + socket.getClass().getName());
      throw new IOException("Socket type is not supported");
    }
  }

  private void doServeSocket(AFUNIXSocket socket) throws IOException{
    if (validateCredentials(socket.getPeerCredentials())) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          logger.info("read command " + line + " from peer with credentials: " + socket.getPeerCredentials());
          Command cmd = null;
          switch (line.trim()) {
            case "stop":
              cmd = new StopCommand();
              break;
            default:
              throw new UnsupportedOperationException("Invalid command");
          }
          cmd.execute(socket);
          logger.info("execution complete");
        }
      }
    } else {
      // do nothing and wait for socket to close since the credentials aren't matching
    }
  }

  private boolean validateCredentials(AFUNIXSocketCredentials credentials) {
    logger.info("Connection from uid: " + credentials.getUid());
    return allowedUids.contains(credentials.getUid());
  }

  private interface Command {
    void execute(AFUNIXSocket socket);
  }

  private static class StopCommand implements Command {

    @Override
    public void execute(AFUNIXSocket socket) {
      logger.info("Stopping all logstreams...");
      try {
        LogStreamManager.getInstance().drainAndStopLogStreams().get();
        logger.info("All logstreams stopped.");
      } catch (Exception e) {
        logger.error("Failed to drain and stop logstreams", e);
      }
      try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {
        writer.write("done");
        writer.flush();
      } catch (IOException ioException) {
        logger.error("Failed to write to socket, terminating process.");
        System.exit(0);
      }
    }
  }

}
