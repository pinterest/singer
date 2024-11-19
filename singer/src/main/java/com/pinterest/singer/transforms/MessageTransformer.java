package com.pinterest.singer.transforms;

/**
 * Represents a message transformer that can be used by LogFileReaders or other components to
 * transform messages.
 */
public interface MessageTransformer<T> {

  /**
   * Transform the message.
   *
   * @param message the message to transform.
   * @return the transformed message.
   */
  public T transform(T message);
}
