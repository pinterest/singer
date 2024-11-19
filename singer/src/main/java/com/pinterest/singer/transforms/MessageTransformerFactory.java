package com.pinterest.singer.transforms;

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.thrift.configuration.MessageTransformerConfig;
import com.pinterest.singer.thrift.configuration.RegexBasedModifierConfig;
import com.pinterest.singer.thrift.configuration.TransformType;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageTransformerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(MessageTransformerFactory.class);

  public static MessageTransformer getTransformer(MessageTransformerConfig transformerConfig, LogStream logStream)
      throws IllegalArgumentException {
    if (transformerConfig == null) {
      return null;
    }
    TransformType transformType = transformerConfig.getType();
    MessageTransformer transformer;
    try {
      switch (transformType) {
        case REGEX_BASED_MODIFIER:
          RegexBasedModifierConfig regexBasedModifierConfig = transformerConfig.getRegexBasedModifierConfig();
          transformer = new RegexBasedModifier(regexBasedModifierConfig, logStream);
          break;
        default:
          transformer = null;
      }
    } catch (ConfigurationException e) {
      LOG.warn("Could not initialize transformer {} for log stream {}, it will be disabled", transformType,
          logStream.getSingerLog().getSingerLogConfig().getName(), e);
      transformer = null;
    }
    return transformer;
  }
}
