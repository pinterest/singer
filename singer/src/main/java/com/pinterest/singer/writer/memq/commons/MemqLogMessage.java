package com.pinterest.singer.writer.memq.commons;

import java.util.HashMap;

public class MemqLogMessage<H, T> extends HashMap<String, Object> {

  private static final long serialVersionUID = 1L;
  public static final String INTERNAL_FIELD_WRITE_TIMESTAMP = "wts";
  public static final String INTERNAL_FIELD_NOTIFICATION_PARTITION_ID = "npi";
  public static final String INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP = "nrts";
  public static final String INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET = "npo";
  public static final String INTERNAL_FIELD_MESSAGE_OFFSET = "mo";

  private final H headers;
  private final T value;

  public MemqLogMessage(H headers, T value) {
    super(5, 1f);
    this.value = value;
    this.headers = headers;
  }

  public T getValue() {
    return value;
  }

  public H getHeaders() {
    return headers;
  }
}
