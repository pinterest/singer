package com.pinterest.singer.monitor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerLog;
import com.pinterest.singer.config.Decider;
import com.pinterest.singer.thrift.configuration.LogStreamProcessorConfig;
import com.pinterest.singer.thrift.configuration.SamplingType;
import com.pinterest.singer.thrift.configuration.SingerConfig;
import com.pinterest.singer.thrift.configuration.SingerLogConfig;

import org.junit.Test;

import java.util.HashMap;

public class DefaultLogMonitorTest {

  @Test
  public void testIsLogStreamInactive() throws Exception {
    SingerConfig singerConfig = new SingerConfig();
    DefaultLogMonitor logMonitor = new DefaultLogMonitor(1, singerConfig);
    LogStreamProcessorConfig lspc = new LogStreamProcessorConfig();
    SingerLogConfig slc = new SingerLogConfig("test", "/tmp", "thrift.log", lspc, null, null);
    slc.setLogDecider("test_decider");
    SingerLog singerLog = new SingerLog(slc);
    LogStream logStream = new LogStream(singerLog, "thrift.log");
    Decider.setInstance(new HashMap<>());

    // should always be active if decider doesn't exist
    for (SamplingType type : new SamplingType[]{SamplingType.NONE, SamplingType.MESSAGE, SamplingType.INSTANCE}) {
      lspc.setDeciderBasedSampling(type);
      for (int i : new int[]{0, 1, 100}) {
        logMonitor.setInstanceLevelSamplingThresholdValue(i);
        assertFalse(logMonitor.isLogStreamInactive(logStream));
      }
    }

    lspc.setDeciderBasedSampling(SamplingType.INSTANCE);

    Decider.getInstance().getDeciderMap().put("test_decider", 0);
    // should always be inactive if decider set to 0
    for (int i : new int[]{0, 1, 100}) {
      logMonitor.setInstanceLevelSamplingThresholdValue(i);
      assertTrue(logMonitor.isLogStreamInactive(logStream));
    }

    // should not be inactive when the threshold value is < 10
    Decider.getInstance().getDeciderMap().put("test_decider", 10);
    for (int i : new int[]{0, 9}) {
      logMonitor.setInstanceLevelSamplingThresholdValue(i);
      assertFalse(logMonitor.isLogStreamInactive(logStream));
    }
    for (int i : new int[]{10, 11, 100}) {
      logMonitor.setInstanceLevelSamplingThresholdValue(i);
      assertTrue(logMonitor.isLogStreamInactive(logStream));
    }

    // should always be active as long as decider is not 0 if the sampling type is not INSTANCE level
    for (SamplingType type : new SamplingType[]{SamplingType.NONE, SamplingType.MESSAGE}) {
      lspc.setDeciderBasedSampling(type);
      Decider.getInstance().getDeciderMap().put("test_decider", 0);
      // should always be inactive if decider set to 0
      for (int i : new int[]{0, 1, 100}) {
        logMonitor.setInstanceLevelSamplingThresholdValue(i);
        assertTrue(logMonitor.isLogStreamInactive(logStream));
      }

      // should not be inactive once the threshold value is >= 10
      Decider.getInstance().getDeciderMap().put("test_decider", 10);
      for (int i : new int[]{0, 9, 10, 11, 100}) {
        logMonitor.setInstanceLevelSamplingThresholdValue(i);
        assertFalse(logMonitor.isLogStreamInactive(logStream));
      }
    }
  }
}