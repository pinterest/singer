package com.pinterest.singer.writer.memq.commons;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.logging.Logger;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.JsonObject;

public class MemqLogMessageIterator<H, T> implements Iterator<MemqLogMessage<H, T>> {

  private static final Logger logger = Logger
      .getLogger(MemqLogMessageIterator.class.getCanonicalName());
  public static final String METRICS_PREFIX = "memqConsumer";
  public static final String MESSAGES_PROCESSED_COUNTER_KEY = METRICS_PREFIX
      + ".messagesProcessedCounter";
  public static final String BYTES_PROCESSED_METER_KEY = METRICS_PREFIX + ".bytesProcessedCounter";
  protected ByteBuffer objectByteBuffer;
  protected DataInputStream uncompressedBatchInputStream;
  protected int objectBytesToRead;
  protected Deserializer<T> valueDeserializer;
  protected Deserializer<H> headerDeserializer;
  protected int messagesToRead;
  protected MetricRegistry metricRegistry;
  protected JsonObject currNotificationObj;
  protected int currentMessageOffset;

  public MemqLogMessageIterator(ByteBuffer objectByteBuffer,
                                JsonObject currNotificationObj,
                                Deserializer<H> headerDeserializer,
                                Deserializer<T> valueDeserializer,
                                MetricRegistry metricRegistry) {
    this.objectByteBuffer = objectByteBuffer;
    this.currNotificationObj = currNotificationObj;
    this.headerDeserializer = headerDeserializer;
    this.valueDeserializer = valueDeserializer;
    this.metricRegistry = metricRegistry;
    readHeaderAndLoadBatch();
  }

  @Override
  public boolean hasNext() {
    // no more notifications
    if (objectBytesToRead <= 0) {
      // last batch remaining to read
      return messagesToRead > 0;
    }
    // more notifications or object not fully read
    return true;
  }

  @Override
  public MemqLogMessage<H, T> next() {
    if (messagesToRead > 0) {
      // still more MemqLogMessages in this batch
      try {
        return getMemqLogMessage();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      // no more MemqLogMessages in this batch
      if (objectBytesToRead > 0) {
        // there are more batches, process new batch
        if (readHeaderAndLoadBatch()) {
          return next();
        }
      }
    }
    throw new RuntimeException(new DataCorruptionException("No next"));
  }

  /**
   * Reads the next batch's header and returns whether there are bytes to read
   *
   * @return true if there are batch bytes to read, false otherwise
   * @throws IOException
   */
  protected boolean readHeaderAndLoadBatch() {
    try {
      currentMessageOffset = 0;
      MemqMessageHeader header = readHeader();
      logger.fine("Message header:" + header);
      byte[] batch = new byte[header.getBatchLength()];
      objectByteBuffer.get(batch);
      if (!CommonUtils.crcChecksumMatches(batch, header.getCrc())) {
        // CRC checksum mismatch
        throw new RuntimeException(new DataCorruptionException("CRC checksum mismatch"));
      }
      uncompressedBatchInputStream = CommonUtils.getUncompressedInputStream(header.getCompression(),
          new ByteArrayInputStream(batch));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return messagesToRead > 0;
  }

  protected MemqMessageHeader readHeader() throws IOException {
    MemqMessageHeader header = new MemqMessageHeader(objectByteBuffer);
    objectBytesToRead -= header.getHeaderLength();
    messagesToRead = header.getLogmessageCount();
    objectBytesToRead -= header.getBatchLength();
    return header;
  }

  /**
   * This method allows skipping to last message of a Batch efficiently. <br/>
   * Using this method allows clients to skip decompression of one message at the
   * time and simply scroll to the last message. <br/>
   * <br/>
   * This method skips over all Messages but the last one and then reads the last
   * message till (N-1)th message leaving the last message to be accessed via the
   * next() method.
   * 
   * @throws IOException
   */
  public void skipToLastLogMessage() throws IOException {
    currentMessageOffset = 0;
    if (objectByteBuffer == null) {
      throw new IOException("");
    }
    while (objectByteBuffer.hasRemaining()) {
      readHeaderAndLoadBatch();
    }
    while (messagesToRead > 1) {
      getMemqLogMessage();
    }
  }

  /**
   * Reads the uncompressed input stream and returns a MemqLogMessage
   *
   * @return a MemqLogMessage
   * @throws IOException
   */
  protected MemqLogMessage<H, T> getMemqLogMessage() throws IOException {
    short logMessageInternalFieldsLength = uncompressedBatchInputStream.readShort();
    long writeTimestamp = 0;
    if (logMessageInternalFieldsLength > 0) {
      byte[] internalFieldBytes = new byte[logMessageInternalFieldsLength];
      uncompressedBatchInputStream.readFully(internalFieldBytes);
      ByteBuffer internalFields = ByteBuffer.wrap(internalFieldBytes);
      writeTimestamp = internalFields.getLong();
      // ###################################################
      // do something with internal headers here in future
      // ###################################################
    }
    int headerLength = uncompressedBatchInputStream.readInt();
    byte[] headerBytes = null;
    if (headerLength > 0) {
      headerBytes = new byte[headerLength];
      uncompressedBatchInputStream.readFully(headerBytes);
    }
    int logMessageBytesToRead = uncompressedBatchInputStream.readInt();
    byte[] logMessageBytes = new byte[logMessageBytesToRead];
    uncompressedBatchInputStream.readFully(logMessageBytes);
    messagesToRead--;
    metricRegistry.counter(MESSAGES_PROCESSED_COUNTER_KEY).inc();
    metricRegistry.meter(BYTES_PROCESSED_METER_KEY).mark(logMessageBytesToRead);
    MemqLogMessage<H, T> logMessage = new MemqLogMessage<>(
        headerDeserializer.deserialize(headerBytes),
        valueDeserializer.deserialize(logMessageBytes));
    populateInternalFields(writeTimestamp, logMessage);
    currentMessageOffset++;
    return logMessage;
  }

  private void populateInternalFields(long writeTimestamp, MemqLogMessage<H, T> logMessage) {
    logMessage.put(MemqLogMessage.INTERNAL_FIELD_WRITE_TIMESTAMP, writeTimestamp);
    logMessage.put(MemqLogMessage.INTERNAL_FIELD_MESSAGE_OFFSET, currentMessageOffset);
    try {
      logMessage.put(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID, currNotificationObj
          .get(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID).getAsInt());
      logMessage.put(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET,
          currNotificationObj.get(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET)
              .getAsLong());
      logMessage.put(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP, currNotificationObj
          .get(MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP).getAsLong());
    } catch (Exception e) {
    }
  }

  @Override
  public void remove() {
    // do nothing
  }

  @Override
  public void forEachRemaining(Consumer<? super MemqLogMessage<H, T>> action) {
    while (hasNext()) {
      action.accept(next());
    }
  }
}
