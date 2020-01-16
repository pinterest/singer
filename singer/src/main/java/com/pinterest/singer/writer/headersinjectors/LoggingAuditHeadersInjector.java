package com.pinterest.singer.writer.headersinjectors;

import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.thrift.LogMessage;
import com.pinterest.singer.writer.HeadersInjector;

import com.twitter.ostrich.stats.Stats;
import org.apache.kafka.common.header.Headers;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LoggingAuditHeadersInjector can extract LoggingAuditHeaders from LogMessage and inject this
 * this object as the headers of ProducerRecord's Headers.
 */
public class LoggingAuditHeadersInjector implements HeadersInjector {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingAuditHeadersInjector.class);
  private static final String HEADER_KEY = "loggingAuditHeaders";

  private final TSerializer SER = new TSerializer();

  public static String getHeaderKey() {
    return HEADER_KEY;
  }

  @Override
  public Headers addHeaders(Headers headers, LogMessage logMessage) {
    try {
      headers.add(HEADER_KEY, SER.serialize(logMessage.getLoggingAuditHeaders()));
    } catch (TException e) {
      Stats.incr(SingerMetrics.NUMBER_OF_SERIALIZING_HEADERS_ERRORS);
      LOG.warn("Exception thrown while serializing loggingAuditHeaders", e);
    } finally {
      return headers;
    }
  }
}
