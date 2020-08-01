package com.pinterest.singer.writer.headersinjectors;

import com.pinterest.singer.writer.HeadersInjector;

import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LoggingAuditHeadersInjector can extract LoggingAuditHeaders from LogMessage and inject this
 * this object as the headers of ProducerRecord's Headers.
 */
public class LoggingAuditHeadersInjector implements HeadersInjector {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingAuditHeadersInjector.class);

  @Override
  public Headers addHeaders(Headers headers, String key, byte[] value) {
    headers.add(key, value);
    return headers;
  }
}
