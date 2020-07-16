package com.pinterest.singer.writer.memq.commons;

public class UnknownCompressionException extends Exception {

  private static final long serialVersionUID = 1L;

  public UnknownCompressionException(String message) {
    super(message);
  }
}