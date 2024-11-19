package com.pinterest.singer.transforms;

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.metrics.OpenTsdbMetricConverter;
import com.pinterest.singer.thrift.configuration.RegexBasedModifierConfig;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * A message transformer that modifies messages based on a regex pattern.
 * This transformer uses a regex pattern to match a message and then replaces the matched groups in
 * the message with the specified format.
 */
public class RegexBasedModifier implements MessageTransformer<ByteBuffer> {
  private static final Logger LOG = LoggerFactory.getLogger(RegexBasedModifier.class);
  private final LogStream logStream;
  private final String logName;
  private final Pattern regex;
  private final Charset encoding;
  private final String messageFormat;
  private final boolean appendNewline;

  public RegexBasedModifier(RegexBasedModifierConfig config, LogStream logStream)
      throws ConfigurationException {
    this.messageFormat = Preconditions.checkNotNull(config.getModifiedMessageFormat());
    this.encoding = Charset.forName(config.getEncoding());
    this.logStream = logStream;
    this.logName = logStream.getSingerLog().getSingerLogConfig().getName();
    this.appendNewline = config.isAppendNewLine();
    try {
      this.regex = Pattern.compile(config.getRegex());
    } catch (PatternSyntaxException e) {
      throw new ConfigurationException("Invalid modifier regex ", e);
    }
  }

  /**
   * Transforms a message based on the regex pattern and message format.
   * If the message does not match the regex pattern or an exception occurs, the original message
   * is returned so that we don't block the log stream. If the message matches the regex pattern,
   * the matched groups are replaced in the message format, each group should be referenced in the
   * modified message format using $1, $2, etc.
   *
   * @param message the message to transform.
   * @return the transformed message or the original message if no transformation occurred.
   */
  @Override
  public ByteBuffer transform(ByteBuffer message) {
    String messageString = bufferToString(message);
    Matcher matcher = regex.matcher(messageString);
    if (!matcher.find()) {
      LOG.debug("[RegexParser] Message " + messageString + " did not match regex: " + regex);
      OpenTsdbMetricConverter.incr(SingerMetrics.REGEX_BASED_MODIFIER + "no_message_match",
          "logName=" + "file=" + logStream.getFileNamePrefix());
      return message;
    }
    try {
      Map<String, String> groupMap = new HashMap<>();
      for (int i = 1; i <= matcher.groupCount(); i++) {
        groupMap.put("$" + i, matcher.group(i));
        LOG.debug("Group " + i + ": " + matcher.group(i));
      }
      StringBuilder result = new StringBuilder(messageFormat);
      groupMap.forEach((groupIndex, value) -> {
        int start;
        while ((start = result.indexOf(groupIndex)) != -1) {
          result.replace(start, start + groupIndex.length(), value);
        }
      });
      if (appendNewline) result.append("\n");
      OpenTsdbMetricConverter.incr(SingerMetrics.REGEX_BASED_MODIFIER + "success",
          "logName=" + logName, "file=" + logStream.getFileNamePrefix());
      return ByteBuffer.wrap(result.toString().getBytes(encoding));
    } catch (Exception e) {
      LOG.warn("Failed to transform message in log stream {}, returning raw message.",
          logName, e);
      OpenTsdbMetricConverter.incr(SingerMetrics.REGEX_BASED_MODIFIER + "failures",
          "logName=" + logName, "file=" + logStream.getFileNamePrefix());
      return message;
    }
  }

  /**
   * Converts a ByteBuffer to a String using the specified encoding.
   *
   * @param message
   * @return the String representation of the ByteBuffer.
   */
  private String bufferToString(ByteBuffer message) {
    String string = new String(message.array(), 0, message.limit(), encoding);
    return string;
  }
}