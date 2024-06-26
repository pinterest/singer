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

import com.pinterest.singer.common.SingerMetrics;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;
import com.pinterest.singer.utils.LogConfigUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.twitter.ostrich.stats.Stats;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.AmazonS3Client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * Directory-based {@link SingerConfigurator} implementation. It constructs the {@link SingerConfig}
 * by parsing the entire config dir.
 */
public class DirectorySingerConfigurator implements SingerConfigurator {

  public static final String SINGER_LOG_CONFIG_PATH = "SINGER_LOG_CONFIG_PATH";
  @VisibleForTesting
  public static final String SINGER_CONFIGURATION_FILE = "singer.properties";
  @VisibleForTesting
  public static final String SINGER_LOG_CONFIG_DIR = System.getenv(SINGER_LOG_CONFIG_PATH)!=null? System.getenv(SINGER_LOG_CONFIG_PATH) : "conf.d";
  @VisibleForTesting
  public static final String DATAPIPELINES_CONFIG = "datapipelines.properties";
  // File name filter. Only files conforming with this pattern will be watched.
  public static final String CONFIG_FILE_PATTERN = ".*\\..*\\.properties";
  private static final Logger LOG = LoggerFactory.getLogger(DirectorySingerConfigurator.class);
  private final File theDir;
  public static final String DATAPIPELINES_CONFIG_S3_BUCKET = "pinterest-shanghai";
  public static final String DATAPIPELINES_CONFIG_S3_KEY = "datapipelines.properties";

  public DirectorySingerConfigurator(String singerConfigDir) throws ConfigurationException {
    theDir = new File(singerConfigDir);
  }

  @Override
  public SingerConfig parseSingerConfig() throws ConfigurationException {
    Preconditions.checkArgument(theDir.isDirectory() && theDir.exists(),
        "Singer configure directory %s is not an existing file directory", theDir.getPath());
    SingerConfig singerConfig = LogConfigUtils.parseDirBasedSingerConfigHeader(FilenameUtils.concat(
        theDir.getPath(), SINGER_CONFIGURATION_FILE));
    // Load the log config dir
    File logConfigDir = new File(FilenameUtils.concat(theDir.getPath(), SINGER_LOG_CONFIG_DIR));
    Preconditions.checkArgument(logConfigDir.exists() && logConfigDir.isDirectory(),
        "%s is not a valid directory", logConfigDir.getPath());
    FileFilter fileFilter = new RegexFileFilter(CONFIG_FILE_PATTERN);
    File[] files = logConfigDir.listFiles(fileFilter);
    Arrays.sort(files);
    List<Consumer<AbstractConfiguration>> overrideConsumers = new ArrayList<>();
    if (singerConfig.isSetConfigOverrideDir()) {
      File configOverrideDir = Paths.get(singerConfig.getConfigOverrideDir()).toFile();
      if (!configOverrideDir.exists() || !configOverrideDir.isDirectory()) {
        throw new ConfigurationException("configOverrideDir " + configOverrideDir + " is not an existing directory");
      }
      File[] overrideConfigs = configOverrideDir.listFiles((FilenameFilter) new SuffixFileFilter(".properties"));
      if (overrideConfigs != null) {
        List<File> sortedOverrideConfigs = Arrays.stream(overrideConfigs).sorted().collect(Collectors.toList());
        for (File f : sortedOverrideConfigs) {
          overrideConsumers.add(generateConsumerFromOverrideConfigFile(f));
        }
      } else {
        LOG.info("No override configs to apply in " + configOverrideDir);
      }
    }
    int badConfigs = 0;
    for (File newFile : files) {
      try {
    	LOG.info("Attempting to parse log config file:" + newFile.getAbsolutePath());
        SingerLogConfig singerLogConfig;
        if (!overrideConsumers.isEmpty()) {
          singerLogConfig = LogConfigUtils.parseLogConfigFromFileWithOverrides(newFile, overrideConsumers);
        } else {
          singerLogConfig = LogConfigUtils.parseLogConfigFromFile(newFile);
        }
        singerConfig.addToLogConfigs(singerLogConfig);
      } catch (Exception e) {
        LOG.error("Failed to parse log config file {}, exception: {}, Skip and continue.",
            newFile.getPath(), ExceptionUtils.getFullStackTrace(e));
        badConfigs++;
      }
    }

    if (badConfigs > 0) {
      Stats.setGauge(SingerMetrics.SINGER_CONFIGURATOR_CONFIG_ERRORS, badConfigs);
      LOG.error("Number of bad log config files is {}", badConfigs);
    }

    // add topic configs from datapipelines.properties
    boolean useNewConfig = (System.getProperty("useNewConfig") != null);
    if(useNewConfig){
      String newLogConfig;

      try {
        newLogConfig = getNewConfig();
      } catch (Exception e) {
        LOG.info("Failed to get {} from S3, exception: {}. Skip and continue.",
            String.format("%s/%s", DATAPIPELINES_CONFIG_S3_BUCKET, DATAPIPELINES_CONFIG_S3_KEY),
            ExceptionUtils.getFullStackTrace(e));
        return singerConfig;
      }

      try {
        SingerLogConfig[] logConfigs = LogConfigUtils.parseLogStreamConfigFromFile(newLogConfig);
        for (SingerLogConfig logConfig : logConfigs) {
          singerConfig.addToLogConfigs(logConfig);
        }
      } catch (ConfigurationException e) {
        Stats.incr(SingerMetrics.SINGER_CONFIGURATOR_CONFIG_ERRORS);
        LOG.info("Failed to parse Singer client config file {}, exception: {}. Terminating",
            DATAPIPELINES_CONFIG, ExceptionUtils.getFullStackTrace(e));
      } catch (Exception e) {
        Stats.incr(SingerMetrics.SINGER_CONFIGURATOR_CONFIG_ERRORS_UNKNOWN);
        LOG.error("Error parsing configuration from file:" + newLogConfig, e);
      }
    }

    return singerConfig;
  }

  /**
   * @return the new config file (datapipelines.properties) from s3 as a String
   */
  private static String getNewConfig () {
    StringBuilder config = new StringBuilder();

    // get config object from S3
    AmazonS3Client s3Client = new AmazonS3Client();
    S3Object configObj = s3Client.getObject(DATAPIPELINES_CONFIG_S3_BUCKET,
        DATAPIPELINES_CONFIG_S3_KEY);

    // write object to String
    BufferedReader reader = new BufferedReader(new InputStreamReader(configObj.getObjectContent()));
    String line;
    try {
      while ((line = reader.readLine()) != null) {
        config.append(String.format("%s\n", line));
      }
    } catch (IOException e) {
      LOG.error("Failed to read config ({}) S3Object to String, exception: {}.",
          String.format("%s/%s", DATAPIPELINES_CONFIG_S3_BUCKET, DATAPIPELINES_CONFIG_S3_KEY),
          ExceptionUtils.getFullStackTrace(e));
    }

    return config.toString();
  }

  private static Consumer<AbstractConfiguration> generateConsumerFromOverrideConfigFile(File file) throws ConfigurationException {
    PropertiesConfiguration overrideConfig = new PropertiesConfiguration(file);
    String matchConfigName = overrideConfig.getString("match.config.name");
    String matchConfigValue = overrideConfig.getString("match.config.value");
    Configuration override = overrideConfig.subset("override");
    return conf -> {
      if (conf.containsKey(matchConfigName) && conf.getProperty(matchConfigName).toString().equals(matchConfigValue)) {
        Iterator<String> itr = override.getKeys();
        while (itr.hasNext()) {
          String key = itr.next();
          conf.setProperty(key, override.getProperty(key));
        }
      }
    };
  }
}
