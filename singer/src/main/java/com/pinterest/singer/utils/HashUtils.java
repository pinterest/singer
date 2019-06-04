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
package com.pinterest.singer.utils;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class HashUtils {

  /**
   * Returns hashed string value of passed in string. Uses MD5
   *
   * @param input
   * @return
   */
  public static String md5HexDigest(String input) {
    return md5HashCode(input).toString();
  }

  public static long md5HashLong(String input) {
    return md5HashCode(input).asLong();
  }

  private static HashCode md5HashCode(String input) {
    @SuppressWarnings("deprecation")
    HashFunction hf = Hashing.md5();
    return hf.newHasher().putString(input, Charsets.UTF_8).hash();
  }

  public static int hashCode(String input) {
    if (Strings.isNullOrEmpty(input)) {
      return 0;
    }

    int hash = 0;
    for (int i = 0; i < input.length(); i++) {
      hash = 31 * hash + input.charAt(i);
    }
    return hash;
  }
}
