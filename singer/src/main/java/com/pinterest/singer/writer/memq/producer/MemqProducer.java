package com.pinterest.singer.writer.memq.producer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.pinterest.singer.writer.memq.commons.Compression;
import com.pinterest.singer.writer.memq.producer.http.MemqHTTPProducer;

public abstract class MemqProducer<H, T> {

  public static final Logger LOG = Logger.getLogger(MemqProducer.class.getCanonicalName());
  public static final int PAYLOADHEADER_BYTES = 8;
  @SuppressWarnings("rawtypes")
  private static Map<String, MemqProducer> clientMap = new ConcurrentHashMap<>();

  public static synchronized <H, T> MemqProducer<H, T> getInstance(String serversetFile,
                                                                   String topicName,
                                                                   int maxInflightRequest,
                                                                   int maxPayLoadBytes,
                                                                   Compression compression,
                                                                   boolean disableAcks,
                                                                   int ackCheckPollInterval,
                                                                   ClientType clientType,
                                                                   String locality,
                                                                   Serializer<H> headerSerializer,
                                                                   Serializer<T> valueSerializer) throws IOException {
    String clientId = serversetFile + "/" + topicName;
    @SuppressWarnings("unchecked")
    MemqProducer<H, T> memqClient = clientMap.get(clientId);
    if (memqClient == null || memqClient.isClosed()) {
      switch (clientType) {
      case HTTP:
        memqClient = new MemqHTTPProducer<>(serversetFile, topicName, maxInflightRequest,
            maxPayLoadBytes, compression, disableAcks, ackCheckPollInterval, locality);
        break;
      case TCP:
//        try {
//          InetSocketAddress socketAddress = tryAndGetAZLocalServer(serversetFile, locality);
//          memqClient = new MemqTCPProducer<>(socketAddress.getHostName(), 9092, topicName,
//              maxInflightRequest, maxPayLoadBytes, compression, disableAcks, ackCheckPollInterval);
//        } catch (Exception e) {
//          LOG.log(Level.SEVERE, "Failed to initialize MemqTCPClient", e);
//        }
        break;
      }
      memqClient.headerSerializer = headerSerializer;
      memqClient.valueSerializer = valueSerializer;
      clientMap.put(clientId, memqClient);
    }
    return memqClient;
  }

  protected static InetSocketAddress tryAndGetAZLocalServer(String serversetFile,
                                                            String locality) throws IOException {
    List<InetSocketAddress> azLocalEndPoints = getLocalServers(serversetFile, locality);
    int randomServer = ThreadLocalRandom.current().nextInt(azLocalEndPoints.size());
    return azLocalEndPoints.get(randomServer);
  }

  public static List<JsonObject> parseServerSetFile(String serversetFile) throws IOException {
    Gson gson = new Gson();
    List<String> lines = Files.readAllLines(new File(serversetFile).toPath());
    return lines.stream().map(line -> gson.fromJson(line, JsonObject.class))
        .filter(g -> g.entrySet().size() > 0).collect(Collectors.toList());
  }

  protected static List<InetSocketAddress> getLocalServers(String serversetFile,
                                                           String locality) throws IOException {
    List<JsonObject> servers = parseServerSetFile(serversetFile);
    if (servers.isEmpty()) {
      throw new IOException("No servers available from serverset:" + serversetFile);
    }
    List<JsonObject> azLocalEndPoints = servers.stream()
        .filter(v -> v.get("az").getAsString().equalsIgnoreCase(locality))
        .collect(Collectors.toList());
    if (azLocalEndPoints.isEmpty()) {
      LOG.warning("Not using AZ awareness due to missing local memq servers for:" + serversetFile);
      azLocalEndPoints = servers;
    }
    return azLocalEndPoints.stream()
        .map(ep -> InetSocketAddress.createUnresolved(ep.get("ip").getAsString(),
            Integer.parseInt(ep.get("port").getAsString())))
        .collect(Collectors.toList());
  }

  protected Serializer<H> headerSerializer;
  protected Serializer<T> valueSerializer;

  public synchronized Future<MemqWriteResult> writeToTopic(H headers, T value) throws IOException {
    byte[] headerBytes = headerSerializer.serialize(headers);
    byte[] valueBytes = valueSerializer.serialize(value);
    int totalPayloadLength = valueBytes.length + (headerBytes != null ? headerBytes.length : 0);
    TaskRequest request = warmAndGetRequestEntry(getCurrentRequestId().get());
    if (totalPayloadLength > getMaxRequestSize()) {
      return getCurrentRequest();
    }
    if (getCurrentRequestTask().remaining() > totalPayloadLength) {
      // note compression estimation isn't used here so we may be leaving unused bytes
      writeMemqLogMessage(headerBytes, valueBytes, request);
      return getCurrentRequest();
    } else {
      finalizeRequest();
      request = warmAndGetRequestEntry(getCurrentRequestId().get());
      writeMemqLogMessage(headerBytes, valueBytes, request);
      return getCurrentRequest();
    }
  }

  protected abstract int getMaxRequestSize();

  public static void writeMemqLogMessage(byte[] headerBytes,
                                         byte[] valueBytes,
                                         TaskRequest request) throws IOException {
    OutputStream os = request.getOutputStream();
    ByteBuffer logMessageInternalFields = ByteBuffer.allocate(2 + 8);
    // presently this is zero but if it's not then additional parsing should be done
    logMessageInternalFields.putShort((short) (logMessageInternalFields.capacity() - 2));
    logMessageInternalFields.putLong(System.currentTimeMillis());
    os.write(logMessageInternalFields.array());
    // ####################################
    // write internal fields here in future
    // ####################################

    ByteBuffer headerLength = ByteBuffer.allocate(4);
    if (headerBytes != null) {
      // mark headers present
      os.write(headerLength.putInt(headerBytes.length).array());
      os.write(headerBytes);
    } else {
      headerLength.putInt(0);
      os.write(headerLength.array());
    }
    os.write(ByteBuffer.allocate(4).putInt(valueBytes.length).array());
    os.write(valueBytes);
    os.flush();
    request.incrementLogMessageCount();
  }

  protected abstract TaskRequest warmAndGetRequestEntry(Long requestId) throws IOException;

  public abstract void finalizeRequest() throws IOException;

  public abstract void close() throws IOException;

  public enum ClientType {
                          HTTP,
                          TCP
  }

  public abstract Future<MemqWriteResult> getCurrentRequest();

  public abstract TaskRequest getCurrentRequestTask();

  public abstract AtomicLong getCurrentRequestId();

  public Serializer<H> getHeaderSerializer() {
    return headerSerializer;
  }

  public void setHeaderSerializer(Serializer<H> headerSerializer) {
    this.headerSerializer = headerSerializer;
  }

  public Serializer<T> getValueSerializer() {
    return valueSerializer;
  }

  public void setValueSerializer(Serializer<T> valueSerializer) {
    this.valueSerializer = valueSerializer;
  }

  public void reinitialize() throws IOException {
  }

  public void cancelAll() {
  }

  public boolean isClosed() {
    // TODO Auto-generated method stub
    return false;
  }
}
