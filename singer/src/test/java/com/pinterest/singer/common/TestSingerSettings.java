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
package com.pinterest.singer.common;

import org.junit.Test;

import com.pinterest.singer.monitor.DefaultLogMonitor;

public class TestSingerSettings {

  /**
   * This test is to ensure that if there are any changes to the static method's
   * interface and no corresponding changes made to the initializer, we can catch
   * this issue as a test failure.
   * 
   * This code can be deprecated once we switch to a factory design pattern.
   * 
   * @throws ClassNotFoundException
   * @throws NoSuchMethodException
   */
  @Test
  public void testLogMonitorMethodInstance() throws ClassNotFoundException, NoSuchMethodException {
    SingerSettings.getLogMonitorStaticInstanceMethod(DefaultLogMonitor.class.getName());
  }

}
