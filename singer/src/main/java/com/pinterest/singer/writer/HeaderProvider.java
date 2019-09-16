package com.pinterest.singer.writer;

import com.pinterest.singer.common.SingerSettings;
import org.apache.kafka.common.header.Headers;

public class HeaderProvider {

  public static final byte[] HOSTNAME = SingerSettings.getEnvironment().getHostname().getBytes();
  public static final byte[] LOCALITY = SingerSettings.getEnvironment().getLocality().getBytes();

  public static void addEnvHeaders(Headers headers){
    headers.add("hostname", HOSTNAME);
    headers.add("locality", LOCALITY);
  }

}
