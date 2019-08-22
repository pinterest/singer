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
package com.pinterest.singer.environment;

import com.pinterest.singer.utils.SingerUtils;

/**
 * This indicates what environment is Singer running in.
 * 
 * Singer Environment indicator can subsequently be used by any component that
 * needs to switch functionality based on the environment it is running in.
 * 
 * NOTE: all variable MUST have default initialized in case the loader doesn't
 * work, all getters must return a NON-NULL value unless NULLs are expected.
 */
public class Environment {

  public static final String LOCALITY_NOT_AVAILABLE = "n/a";
  public static final String DEFAULT_HOSTNAME = SingerUtils.getHostname();
  private String locality = LOCALITY_NOT_AVAILABLE;
  private String deploymentStage;
  private String hostname = DEFAULT_HOSTNAME;

  /**
   * @return the locality
   */
  public String getLocality() {
    return locality;
  }

  /**
   * @param locality the locality to set
   */
  public void setLocality(String locality) {
    this.locality = locality;
  }

  /**
   * @return the deploymentStage
   */
  public String getDeploymentStage() {
    return deploymentStage;
  }

  /**
   * @param deploymentStage the deploymentStage to set
   */
  public void setDeploymentStage(String deploymentStage) {
    this.deploymentStage = deploymentStage;
  }

  /**
   * @return the hostname
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * @param hostname the hostname to set
   */
  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

}