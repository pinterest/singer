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

import com.pinterest.singer.utils.HashUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.twitter.ostrich.stats.Stats;
import com.twitter.util.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Basic Decider Framework.
 * Uses the zookeeper file based config to parse in new decider configurations.
 *
 * Usage:
 *
 * Decider.getInstance().isIdInExperiment(293849384, "pull_homefeed_reads");
 * Decider.getInstance().decideExperiment("pull_homefeed_reads");
 */
public class Decider {

  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(Decider.class);
  private static final String DECIDER_FILE_PATH = "/var/config/config.manageddata.admin.decider";
  private static final String DECIDER_HASH_FORMAT = "decider_%s%s";
  private static final BigInteger BIG_INTEGER_ONE_HUNDRED = BigInteger.valueOf(100);

  // Base decider instance.
  private static volatile Decider instance = null;
  private volatile Map<String, Integer> mDeciderMap = null;
  private final Random rand = new Random();

  @VisibleForTesting
  Decider(ConfigFileWatcher watcher, String filePath) {
    try {
      watcher.addWatch(filePath,
          new Function<byte[], Void>() {
            public Void apply(byte[] deciderJson) {
              Map<String, Integer> newDeciderMap =
                  GSON.fromJson(new String(deciderJson),
                      new TypeToken<HashMap<String, Integer>>() {}
                          .getType());
              if (newDeciderMap != null) {
                if (newDeciderMap.isEmpty()) {
                  LOG.warn("Got empty decider set.");
                }
                mDeciderMap = newDeciderMap;
              } else {
                LOG.warn("Got a null object from decider json.");
              }
              return null;
            }
          });
    } catch (IOException e) {
      // Initialize with an empty map.
      mDeciderMap = Maps.newHashMap();
      LOG.warn("Exception while initializing decider.", e);
      Stats.incr("decider_config_file_ioexception");
    }
    this.rand.setSeed(0);
  }

  /**
   * Initialize a static decider with a prebaked set of decider values.
   */
  @VisibleForTesting
  Decider(Map<String, Integer> deciderMap) {
    this.rand.setSeed(0);
    this.mDeciderMap = deciderMap;
  }

  private Decider() {
    this(ConfigFileWatcher.defaultInstance(), DECIDER_FILE_PATH);
  }

  @VisibleForTesting
  public Map<String, Integer> getDeciderMap() {
    return mDeciderMap;
  }

  /**
   * Looks up the value of the decider variable named {@code deciderName} and flips a coin to
   * determine if we should be in the experiment based on the specified ID. Useful if a stable
   * experiment decision is needed for an ID.
   *
   * @param id          ID to be modded in order to figure out if in the experiment or not
   * @param deciderName name of experiment decider to lookup
   * @return true if coin flip put us in the experiment, false otherwise
   */
  public boolean isIdInExperiment(long id, String deciderName) {
    deciderName = deciderName.toLowerCase();
    Integer value = mDeciderMap.get(deciderName);
    if (value == null) {
      return false;
    }
    return decideValueWithId(id, value);
  }

  /**
   * This function computes a hash of the idStr and the decider, so different deciders will
   * get different random samples of ids.
   *
   * It provides the same function as is_hashed_id_in_experiment in python code to make sure
   * that the same key will get the same decider result for both code base.
   *
   * @param idStr          idStr to be hashed
   * @param deciderName name of experiment decider to lookup
   * @return true if coin flip put us in the experiment, false otherwise
   */
  public boolean isHashedIdInExperiment(String idStr, String deciderName) {
    deciderName = deciderName.toLowerCase();
    Integer value = mDeciderMap.get(deciderName);
    if (value == null) {
      return false;
    }

    BigInteger b = new BigInteger(
        HashUtils.md5HexDigest(String.format(DECIDER_HASH_FORMAT, deciderName, idStr)), 16);
    long mod = b.mod(BIG_INTEGER_ONE_HUNDRED).longValue();
    return mod < value;
  }

  /**
   * Looks up the value of the decider variable named {@code deciderName} and flips a coin to
   * determine if we should be in the experiment.
   *
   * @param deciderName name of decider variable to lookup
   * @return true if coin flip put us in the experiment, false otherwise
   */
  public boolean decideExperiment(String deciderName) {
    deciderName = deciderName.toLowerCase();
    Integer value = mDeciderMap.get(deciderName);
    if (value == null) {
      return false;
    }
    return decideValue(value);
  }

  /**
   * Looks up the value of the decider variable named {@code deciderName} and flips a coin to
   * determine if we should be in the experiment.
   *
   * @param deciderName   name of decider variable to lookup
   * @param defaultValue  if no decider variable {@code deciderName} exists, value to return
   * @return              {@code defaultValue} if decider variable {@code deciderName} does not
   *                      exist.  Otherwise, outcome whether coin flip put us in the experiment.
   */
  public boolean decideExperiment(String deciderName, boolean defaultValue) {
    deciderName = deciderName.toLowerCase();
    if (!mDeciderMap.containsKey(deciderName)) {
      return defaultValue;
    }
    return decideExperiment(deciderName);
  }

  /**
   * Randomly determine if we should be in the proposed group given a specified probability
   * percentage (0 - 100). E.g. if the input value is 0, we will always return false. If the value
   * is 1, we will return true 1% of the time, 99% of the time, etc.
   *
   * @param deciderValue  percentage (0 - 100), represents probability of returning true
   * @return yes if randomized result agreed with the proposed value
   */
  public boolean decideValue(int deciderValue) {
    // We get a random number from 1 - 100.
    int number = rand.nextInt(100) + 1;
    return number <= deciderValue;
  }

  /**
   * return the value of the decider variable named {@code deciderName}.
   * return -1 if the deciderName is not defined.
   *
   * @param deciderName   name of decider variable to lookup
   * @return
   */
  public int getDeciderValue(String deciderName) {
    return getDeciderValue(deciderName, -1);
  }

  /**
   * return the value of the decider variable named {@code deciderName}.
   * return -1 if the deciderName is not defined.
   *
   * @param deciderName   name of decider variable to lookup
   * @param defaultValue  if no decider variable {@code deciderName} exists, value to return
   * @return
   */
  public int getDeciderValue(String deciderName, int defaultValue) {
    deciderName = deciderName.toLowerCase();
    if (mDeciderMap.containsKey(deciderName)) {
      return mDeciderMap.get(deciderName);
    }
    return defaultValue;
  }

  /**
   * Determine if the specified id should be in experiment group given its decider value.
   *
   * @param id            id to mod and compare with deciderValue
   * @param deciderValue  percentage (0 - 100), represents probability of returning true
   * @return yes if randomized result agreed with the proposed value
   */
  public boolean decideValueWithId(long id, int deciderValue) {
    int mod = (int) (Math.abs(id) % 100);
    return mod < deciderValue;
  }

  public static Decider getInstance() {
    if (instance == null) {
      synchronized (Decider.class) {
        if (instance == null) {
          instance = new Decider();
          return instance;
        }
      }
    }
    return instance;
  }

  /**
   * Override the singleton instance with @deciderMap. This is a public method
   * available for testing purposes. This is public since other packages using decider
   * framework need it for testing their code.
   */
  @VisibleForTesting
  public static void setInstance(Map<String, Integer> deciderMap) {
    instance = new Decider(deciderMap);
  }

  /**
   * Exception for when a decider has been turned off
   */
  public static class DeciderOffException extends RuntimeException {
    public DeciderOffException(String deciderName, String customizedErrorMessage) {
      super(Strings.isNullOrEmpty(customizedErrorMessage)
            ? String.format("Decider %s has been turned off", deciderName)
            : customizedErrorMessage);
    }

    public DeciderOffException(String exceptionMessage) {
      super(exceptionMessage);
    }
  }

  public static class ServiceDeciderOffException extends DeciderOffException {
    public ServiceDeciderOffException(String serviceName) {
      super(String.format("Service %s has been decidered off", serviceName));
    }
  }
}
