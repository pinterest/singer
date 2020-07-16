package com.pinterest.singer.writer.memq.producer.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.singer.writer.memq.commons.Compression;
import com.pinterest.singer.writer.memq.commons.MemqMessageHeader;
import com.pinterest.singer.writer.memq.producer.MemqWriteResult;
import com.pinterest.singer.writer.memq.producer.TaskRequest;

public class HttpWriteTaskRequest extends TaskRequest {

  private static final Logger LOG = LoggerFactory.getLogger(HttpWriteTaskRequest.class);
  private static final String USERAGENT_NAME = "singer:";
  private static final int READY_CHECK_FREQUENCY = 100;
  private volatile boolean ready = false;
  private String baseUrl;
  private ByteArrayOutputStream buf;
  private long clientRequestId;
  private long serverRequestId;
  private CloseableHttpClient client;
  private Semaphore maxRequestLock;
  private int maxPayLoadBytes;
  private Map<Long, TaskRequest> requestMap;
  private int ackCheckPollIntervalMs;
  private boolean disableAcks;
  private OutputStream outputStream;
  private byte[] byteArrays;
  private MemqMessageHeader header = new MemqMessageHeader(this);

  public HttpWriteTaskRequest(String baseUrl,
                              long currentRequestId,
                              Compression compression,
                              Semaphore maxRequestLock,
                              boolean disableAcks,
                              int maxPayLoadBytes,
                              int ackCheckPollIntervalMs,
                              Map<Long, TaskRequest> requestMap) throws IOException {
    this.baseUrl = baseUrl;
    this.compression = compression;
    buf = new ByteArrayOutputStream(maxPayLoadBytes - 8192);
    outputStream = prepareOutputStream(buf);
    this.clientRequestId = currentRequestId;
    this.maxRequestLock = maxRequestLock;
    this.disableAcks = disableAcks;
    this.maxPayLoadBytes = maxPayLoadBytes;
    this.ackCheckPollIntervalMs = ackCheckPollIntervalMs;
    this.requestMap = requestMap;
  }

  public void markReady() throws IOException {
    outputStream.close();
    byteArrays = buf.toByteArray();
    header.writeHeader(byteArrays);
    this.ready = true;
  }

  public int remaining() {
    return maxPayLoadBytes - buf.size();
  }

  public int size() {
    return buf.size();
  }

  public long getId() {
    return clientRequestId;
  }

  @Override
  public MemqWriteResult call() throws Exception {
    while (!ready) {
      // wait while this request is ready to be processed
      Thread.sleep(READY_CHECK_FREQUENCY);
    }
    LOG.info("Making request waiting for semaphore:" + clientRequestId);
    maxRequestLock.acquire();
    try {
      client = HttpClientBuilder.create().setUserAgent(USERAGENT_NAME).build();
      long writeTs = System.currentTimeMillis();
      serverRequestId = makeRequest(byteArrays);
      int writeLatency = (int) (System.currentTimeMillis() - writeTs);

      LOG.debug("Request made with server request id: {} and client request id: {}",
          serverRequestId, clientRequestId);
      outputStream = null;
      buf = null;

      int ackLatency = 0;
      while (!disableAcks && !checkComplete()) {
        // wait for this request to signal processed
        Thread.sleep(ackCheckPollIntervalMs);
        ackLatency += ackCheckPollIntervalMs;
      }
      client.close();
      if (!disableAcks) {
        LOG.info("Request acknowledged:" + clientRequestId + " in:" + ackLatency + "ms");
      } else {
        LOG.info("Skipping acknowledgement:" + clientRequestId);
      }
      return new MemqWriteResult(clientRequestId, writeLatency, ackLatency, byteArrays.length);
    } catch (Exception e) {
      LOG.error("Request failed clientRequestId(" + clientRequestId + ") serverRequestId("
          + serverRequestId + ")", e);
      throw e;
    } finally {
      requestMap.remove(clientRequestId);
      maxRequestLock.release();
    }
  }

  private long makeRequest(byte[] payload) throws ClientProtocolException, IOException {
    LOG.info("Making request to URL: " + baseUrl + clientRequestId);
    HttpPost post = new HttpPost(baseUrl + clientRequestId);
    post.setEntity(new ByteArrayEntity(payload));
    CloseableHttpResponse response = client.execute(post);
    StatusLine statusLine = response.getStatusLine();
    if (statusLine.getStatusCode() == 200) {
      long serverRequestId = Long.parseLong(EntityUtils.toString(response.getEntity()));
      return serverRequestId;
    } else {
      throw new IOException("Request failed due to with status:" + statusLine);
    }
  }

  private boolean checkComplete() throws Exception {
    try {
      String uri = baseUrl + "status/" + clientRequestId + "/" + serverRequestId;
      LOG.debug("Check batch request:" + uri);
      CloseableHttpResponse response = client.execute(new HttpGet(uri));
      if (response.getStatusLine().getStatusCode() == 204) {
        // processed
        return true;
      } else if (response.getStatusLine().getStatusCode() == 202) {
        return false;
      } else {
        throw new Exception("Request failed with code:" + response.getStatusLine());
      }
    } catch (Exception e) {
      throw e;
    }
  }

  public OutputStream getOutputStream() {
    return outputStream;
  }

  @VisibleForTesting
  public byte[] getByteArrays() {
    return byteArrays;
  }

  @VisibleForTesting
  @Override
  public MemqMessageHeader getHeader() {
    return header;
  }

  @VisibleForTesting
  @Override
  public short getVersion() {
    return 1_0_0;
  }
}