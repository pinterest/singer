package com.pinterest.singer.writer.memq.producer.http;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.pinterest.singer.writer.memq.commons.Compression;
import com.pinterest.singer.writer.memq.producer.MemqProducer;
import com.pinterest.singer.writer.memq.producer.MemqWriteResult;
import com.pinterest.singer.writer.memq.producer.TaskRequest;

public class MemqHTTPProducer<H, T> extends MemqProducer<H, T> {

  private static final Logger LOG = Logger.getLogger(MemqHTTPProducer.class.getCanonicalName());
  private Map<Long, TaskRequest> requestMap;
  private int maxPayLoadBytes;
  private ExecutorService es;
  private AtomicLong currentRequestId;
  private Future<MemqWriteResult> currentRequest;
  private TaskRequest currentRequestTask;
  private int maxInflightRequests;
  private Semaphore maxRequestLock;
  private Compression compression;
  private int ackCheckPollIntervalMs;
  private boolean disableAcks;
  private String topicName;
  private List<InetSocketAddress> localServers;
  private String serversetFile;
  private String locality;

  public MemqHTTPProducer(String serversetFile,
                          String topicName,
                          int maxInflightRequest,
                          int maxPayLoadBytes,
                          Compression compression,
                          boolean disableAcks,
                          int ackCheckPollIntervalMs,
                          String locality) throws IOException {
    this.serversetFile = serversetFile;
    this.topicName = topicName;
    this.compression = compression;
    this.disableAcks = disableAcks;
    this.ackCheckPollIntervalMs = ackCheckPollIntervalMs;
    this.locality = locality;
    this.requestMap = new ConcurrentHashMap<>();
    this.maxInflightRequests = maxInflightRequest;
    this.es = Executors.newFixedThreadPool(maxInflightRequests,
        new DaemonThreadFactory("MemqRequestPool-" + topicName + "-"));
    this.currentRequestId = new AtomicLong(Math.abs(UUID.randomUUID().getMostSignificantBits()));
    this.maxRequestLock = new Semaphore(maxInflightRequests);
    this.maxPayLoadBytes = maxPayLoadBytes;
    this.localServers = getLocalServers(serversetFile, locality);

    LOG.warning("Creating SimpleMemqClient with url:" + serversetFile + " maxPayLoadBytes:"
        + maxPayLoadBytes + " maxInflightRequests:" + maxInflightRequest + " compression:"
        + compression);
  }

  @Override
  public synchronized void finalizeRequest() throws IOException {
    LOG.fine("Buffer filled:" + (double) currentRequestTask.size() / 1024 / 1024);
    currentRequestTask.markReady();
    currentRequestId.incrementAndGet();
  }

  public synchronized MemqWriteResult writeToTopicNoBatch(List<byte[]> payloads) throws InterruptedException,
                                                                                 ExecutionException,
                                                                                 IOException {
    long requestId = currentRequestId.incrementAndGet();
    TaskRequest request = warmAndGetRequestEntry(requestId);
    OutputStream buf = request.getOutputStream();
    for (byte[] payload : payloads) {
      buf.write(payload);
    }
    currentRequestTask.markReady();
    return currentRequest.get();
  }

  @Override
  public synchronized TaskRequest warmAndGetRequestEntry(Long requestId) throws IOException {
    TaskRequest request = requestMap.get(requestId);
    if (request == null) {
      String baseUrl = getServerUrl();
      currentRequestTask = new HttpWriteTaskRequest(baseUrl, currentRequestId.get(), compression,
          maxRequestLock, disableAcks, maxPayLoadBytes, ackCheckPollIntervalMs, requestMap);
      request = currentRequestTask;
      requestMap.put(requestId, currentRequestTask);
      currentRequest = es.submit(currentRequestTask);
      LOG.info("Creating new request holder, clientRequestId(" + currentRequestTask.getId()
          + "), current pending req:" + requestMap.size());
    }
    return request;
  }

  public String getServerUrl() throws IOException {
    int randomServerIndex = ThreadLocalRandom.current().nextInt(localServers.size());
    InetSocketAddress socketAddress = localServers.get(randomServerIndex);
    return "http://" + socketAddress.getHostName() + ":" + socketAddress.getPort() + "/api/topic/"
        + topicName + "/";
  }

  @Override
  public synchronized void reinitialize() throws IOException {
    this.localServers = getLocalServers(serversetFile, locality);

  }

  @Override
  public void close() {
    es.shutdownNow();
    requestMap.clear();
  }

  @Override
  public boolean isClosed() {
    return es.isShutdown();
  }

  @Override
  public TaskRequest getCurrentRequestTask() {
    return currentRequestTask;
  }

  @Override
  public Future<MemqWriteResult> getCurrentRequest() {
    return currentRequest;
  }

  @Override
  public AtomicLong getCurrentRequestId() {
    return currentRequestId;
  }

  @Override
  protected int getMaxRequestSize() {
    return maxPayLoadBytes;
  }
}