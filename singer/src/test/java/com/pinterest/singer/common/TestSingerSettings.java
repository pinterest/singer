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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import com.pinterest.singer.environment.EnvVariableBasedEnvironmentProvider;
import com.pinterest.singer.monitor.DefaultLogMonitor;
import com.pinterest.singer.thrift.configuration.SingerConfig;

public class TestSingerSettings {

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

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

  @Test
  public void testEnvironmentLoader() {
    SingerConfig config = new SingerConfig();
    SingerSettings.loadAndSetSingerEnvironmentIfConfigured(config);
    assertNotNull(SingerSettings.getEnvironment());
    // check by default environment provider class is null
    assertNull(config.getEnvironmentProviderClass());
    assertEquals(null, SingerSettings.getEnvironment().getDeploymentStage());

    // bad class name should cause an exception to be logged but MUST not throw an
    // error and environment should still be correct
    config.setEnvironmentProviderClass("com.pinterest.xyz");
    SingerSettings.loadAndSetSingerEnvironmentIfConfigured(config);
    assertNotNull(SingerSettings.getEnvironment());
    assertEquals(null, SingerSettings.getEnvironment().getDeploymentStage());

    environmentVariables.set("DEPLOYMENT_STAGE", "CANARY");
    environmentVariables.set("LOCALITY", "us-east-1a");
    config.setEnvironmentProviderClass(EnvVariableBasedEnvironmentProvider.class.getName());
    SingerSettings.loadAndSetSingerEnvironmentIfConfigured(config);
    assertNotNull(SingerSettings.getEnvironment());
    assertEquals("CANARY", SingerSettings.getEnvironment().getDeploymentStage());
    assertEquals("us-east-1a", SingerSettings.getEnvironment().getLocality());
    
    environmentVariables.set("DEPLOYMENT_STAGE", "DEV");
    config.setEnvironmentProviderClass(EnvVariableBasedEnvironmentProvider.class.getName());
    SingerSettings.loadAndSetSingerEnvironmentIfConfigured(config);
    assertNotNull(SingerSettings.getEnvironment());
    assertEquals("DEV", SingerSettings.getEnvironment().getDeploymentStage());
    assertEquals("us-east-1a", SingerSettings.getEnvironment().getLocality());
  }

}
