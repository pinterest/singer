package com.pinterest.singer.writer.memq.commons;

public class DataCorruptionException extends Exception {

  private static final long serialVersionUID = 1L;

  public DataCorruptionException(String message) {
    super(message);
  }
}