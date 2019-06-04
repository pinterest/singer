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
package com.pinterest.singer.writer.kafka.localityaware;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.util.EC2MetadataUtils;
import com.pinterest.singer.common.SingerMetrics;
import com.twitter.ostrich.stats.Stats;

/**
 * Uses AWS SDK to query Local EC2 Metadata Service and find the AZ for this
 * host.
 * 
 * hosts.
 */
public class EC2LocalityInfoProvider implements LocalityInfoProvider {

  private static final Logger LOG = LoggerFactory.getLogger(EC2LocalityInfoProvider.class);
  private String cachedLocality;

  @Override
  public void init(Map<String, String> conf) throws IllegalArgumentException, IOException {
    // check if EC2 AZ info is working
    try {
      EC2MetadataUtils.getAvailabilityZone();
      // cache locality info to avoid service based lookups
      cachedLocality = EC2MetadataUtils.getAvailabilityZone();
    } catch (Exception e) {
      LOG.error("Failed to get AZ info", e);
      Stats.addMetric(SingerMetrics.LOCALITY_MISSING, 1);
      cachedLocality = LOCALITY_NOT_AVAILABLE;
    }
  }

  @Override
  public String getLocalityInfoForLocalhost() {
    return cachedLocality;
  }

}