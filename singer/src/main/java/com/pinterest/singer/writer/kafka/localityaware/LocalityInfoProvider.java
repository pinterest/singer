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

/**
 * Provides locality information to Singer. Initializer MUST call the
 * init(config) method before using the instance.
 * 
 * NOTE: Locality can be synonymous with a Data Center or Rack or an
 * Availability Zone (cloud)
 */
public interface LocalityInfoProvider {
  
  public static final String LOCALITY_NOT_AVAILABLE = "n/a";

  void init(Map<String, String> config) throws IOException;

  String getLocalityInfoForLocalhost() throws IOException;

}