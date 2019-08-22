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

/**
 * Uses environment variables to specify the provider. This class defaults to
 * Environment.PROD unless the environment variable is explicitly configured.
 */
public class EnvVariableBasedEnvironmentProvider extends EnvironmentProvider {

  private static final String DEPLOYMENT_STAGE = "DEPLOYMENT_STAGE";
  private static final String LOCALITY = "LOCALITY";

  @Override
  protected String getLocality() {
    String locality = System.getenv(LOCALITY);
    if (locality != null) {
      return locality;
    } else {
      return Environment.LOCALITY_NOT_AVAILABLE;
    }
  }

  @Override
  protected String getDeploymentStage () {
    return System.getenv(DEPLOYMENT_STAGE);
  }
  
}