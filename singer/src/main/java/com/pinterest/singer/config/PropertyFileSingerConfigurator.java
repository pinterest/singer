/**
 * Copyright 2019 Pinterest, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.singer.config;

import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.utils.LogConfigUtils;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PropertyFileSingerConfigurator will get singer configuration from property files.
 */
public class PropertyFileSingerConfigurator implements SingerConfigurator {

  private static final Logger LOG = LoggerFactory.getLogger(PropertyFileSingerConfigurator.class);

  private final String singerPropertiesFile;

  public PropertyFileSingerConfigurator(String singerPropertiesFile) throws
                                                                     ConfigurationException {
    this.singerPropertiesFile = singerPropertiesFile;
  }

  @Override
  public SingerConfig parseSingerConfig() throws ConfigurationException {
    return LogConfigUtils.parseFileBasedSingerConfig(singerPropertiesFile);
  }
}
