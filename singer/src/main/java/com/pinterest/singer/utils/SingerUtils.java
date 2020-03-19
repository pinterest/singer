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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.apache.commons.io.comparator.NameFileComparator;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.config.DirectorySingerConfigurator;
import com.pinterest.singer.config.PropertyFileSingerConfigurator;
import com.pinterest.singer.config.SingerConfigurator;
import com.pinterest.singer.config.SingerDirectoryWatcher;
import com.pinterest.singer.monitor.LogStreamManager;
import com.pinterest.singer.thrift.configuration.SingerConfig;

/**
 * The utility methods for Singer
 */
public class SingerUtils {
	
  private static String OS = System.getProperty("os.name").toLowerCase();
  private static final boolean IS_MAC = OS.indexOf("mac") >= 0;
  private static final Logger LOG = LoggerFactory.getLogger(SingerUtils.class);

  public static final FileSystem defaultFileSystem = FileSystems.getDefault();

  public static String getHostname() {
    String hostName;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
      int firstDotPos = hostName.indexOf('.');
      if (firstDotPos > 0) {
        hostName = hostName.substring(0, firstDotPos);
      }
    } catch (Exception e) {
      // fall back to env var.
      hostName = System.getenv("HOSTNAME");
    }
    return hostName;
  }

  public static Path getPath(String filePathStr) {
    return defaultFileSystem.getPath(filePathStr);
  }
  /**
   * Conver a string in HH:MM format to an integer in minutes
   * @param timeString  in HH:mm format
   * @return a Date object that presents HH:mm
   */
  public static Date convertToDate(String timeString) throws ConfigurationException {
    SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm");
    Date retval;
    try {
      retval = dateFormat.parse(timeString);
    } catch (ParseException e) {
      throw new ConfigurationException("Invalid HH:MM time string : " + timeString);
    }
    return retval;
  }
  
  public static void printStackTrace() {
	  LOG.warn(Arrays.toString(Thread.currentThread().getStackTrace()));
  }
  
  /**
   * Convert a {@link ByteBuffer} to byte array.
   * Reads all bytes from current position to the limit of the buffer into a byte array.
   * @param buf
   * @return
   */
  public static byte[] readFromByteBuffer(ByteBuffer buf) {
    byte[] bytes = new byte[buf.limit()-buf.position()];
    buf.get(bytes);
    return bytes;
  }

  /**
   * Extracts a long representing an inode number from a Unix FileKey
   *
   * The following code shows how the string representation of a UnixFileKey is generated:
   *   StringBuilder sb = new StringBuilder();
   *   sb.append("(dev=")
   *     .append(Long.toHexString(st_dev))
   *     .append(",ino=")
   *     .append(st_ino)
   *     .append(')');
   *   return sb.toString();
   *
   * So, in order to get the inode number, we will parse the string.
   *
   * @param filePath the path to a file
   * @return The inode number of that file
   */
  public static long getFileInode(Path filePath) throws IOException {
    BasicFileAttributes attrs = Files.readAttributes(filePath, BasicFileAttributes.class);
    Object fileKey = attrs.fileKey();
    String keyStr = fileKey.toString();
    String inodeStr = keyStr.substring(keyStr.indexOf("ino=") + 4, keyStr.indexOf(")"));
    return Long.parseLong(inodeStr);
  }

  public static long getFileInode(String filePathStr) throws IOException {
    Path filePath = defaultFileSystem.getPath(filePathStr);
    return getFileInode(filePath);
  }

  public static long getFileLastModifiedTime(String  filePathStr) {
    File file = new File(filePathStr);
    return getFileLastModifiedTime(file);
  }

  public static long getFileLastModifiedTime(File file) {
    return file.exists() ? file.lastModified() : -1L;
  }

  public static long getFileLastModifiedTime(Path path) {
    return getFileLastModifiedTime(path.toFile());
  }

  public static WatchKey registerWatchKey(WatchService watchService, Path logDir) throws IOException {
    WatchKey watchKey;
    // make watcher more senstive for Mac OS X; this should reduce the poll latency from
    // 10s to 2s since native OS X implementation is not available in JDK
    if(SingerUtils.isMac()) {
      watchKey = logDir.register(watchService,
          new WatchEvent.Kind[]{
              StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE,
              StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.OVERFLOW
          },
          com.sun.nio.file.SensitivityWatchEventModifier.HIGH);
    } else {
      watchKey = logDir.register(watchService,
          StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE,
          StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.OVERFLOW);
    }
    return watchKey;
  }

  /**
   * Reads the Singer configuration from a configuration directory or a properties file. If both a directory and a
   * properties file are specified, the directory will be used.
   * @param singerConfigDir A directory containing Singer config files
   * @param singerPropertiesFile A properties file contains the singer configuration
   * @param startDirectoryWatcher Indicates whether the SingerDirectoryWatcher should be started after the configuration
   *                              is read in.
   * @return The configuration that was read.
   * @throws Exception if neither singerConfigDir or singerPropertiesFile is specified.
     */
  public static SingerConfig loadSingerConfig(String singerConfigDir,
            String singerPropertiesFile, boolean startDirectoryWatcher) throws Exception {
    SingerConfig singerConfig = null;

    // "singer.config.dir" will have precedence over "config".
    if (singerConfigDir != null) {
      LOG.info("Use dir config : " + singerConfigDir);
      SingerConfigurator singerConfigurator = new DirectorySingerConfigurator(singerConfigDir);
      singerConfig = singerConfigurator.parseSingerConfig();
      if (startDirectoryWatcher) {
        SingerSettings.directoryWatcher =
                new SingerDirectoryWatcher(singerConfig, singerConfigurator);
      }
    } else if (singerPropertiesFile != null) {
      LOG.info("Use file config : " + singerPropertiesFile);
      Preconditions.checkNotNull(singerPropertiesFile);
      singerConfig = new PropertyFileSingerConfigurator(singerPropertiesFile).parseSingerConfig();
    } else {
      throw new Exception("Both system properties singer.config.dir and config are undefined");
    }

    LOG.info("Singer config loaded : " + singerConfig);
    return singerConfig;
  }
  
  public static void exit(String msg, int statusCode) {
    LOG.warn(msg);
    System.exit(statusCode);
  }
  
  /** 
   * Copied from https://github.com/srotya/sidewinder/blob/development/core/src/test/java/com/srotya/sidewinder/core/qa/TestUtils.java
   * under Apache 2.0 license
   */
  public static CloseableHttpResponse makeRequest(HttpRequestBase request)
      throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, IOException {
	  return buildClient(request.getURI().toURL().toString(), 1000, 1000, null).execute(request);
  }

  /** 
   * Copied from https://github.com/srotya/sidewinder/blob/development/core/src/test/java/com/srotya/sidewinder/core/qa/TestUtils.java
   * under Apache 2.0 license
   */
  public static CloseableHttpResponse makeRequestAuthenticated(HttpRequestBase request, CredentialsProvider provider)
	throws KeyManagementException, ClientProtocolException, NoSuchAlgorithmException, KeyStoreException,
	MalformedURLException, IOException {
	  return buildClient(request.getURI().toURL().toString(), 1000, 1000, provider).execute(request);
  }

  /** 
   * Copied from https://github.com/srotya/sidewinder/blob/development/core/src/test/java/com/srotya/sidewinder/core/qa/TestUtils.java
   * under Apache 2.0 license
   */
  public static CloseableHttpClient buildClient(String baseURL, int connectTimeout, int requestTimeout,
	CredentialsProvider provider) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
	  HttpClientBuilder clientBuilder = HttpClients.custom();
	  if (provider != null) {
		  clientBuilder.setDefaultCredentialsProvider(provider);
	  }
	  RequestConfig config = RequestConfig.custom().setConnectTimeout(connectTimeout)
		.setConnectionRequestTimeout(requestTimeout).setAuthenticationEnabled(true).build();
	  return clientBuilder.setDefaultRequestConfig(config).build();
  }
  
  public static boolean isMac() {
      return IS_MAC;
  }
  
  public static boolean createEmptyDotFile(String path) throws IOException {
      return new File(path).createNewFile();
  }

  // Compare the file first by last_modified timestamp and then by name in case two files have
  // the same mtime due to precision (mtime is up to seconds).
  public static class LogFileComparator implements Comparator<File> {
    public int compare(File file1, File file2) {
      int lastModifiedTimeComparison = LastModifiedFileComparator.LASTMODIFIED_COMPARATOR.compare(file1, file2);
      if (lastModifiedTimeComparison != 0) {
        return lastModifiedTimeComparison;
      }

      int fileNameLengthComparsion = file2.getName().length() - file1.getName().length();
      if (fileNameLengthComparsion != 0) {
        return fileNameLengthComparsion;
      }

      return NameFileComparator.NAME_REVERSE.compare(file1, file2);
    }
  }
  
  /**
   * Make an HTTP Get request on the supplied URI and return the response entity
   * as {@link String}
   * 
   * @param uri
   * @return
   * @throws IOException
   */
  public static String makeGetRequest(String uri) throws IOException {
      HttpGet getPodRequest = new HttpGet(uri);
      try {
          CloseableHttpResponse response = SingerUtils.makeRequest(getPodRequest);
          if (response.getStatusLine().getStatusCode() != 200) {
              LOG.warn("Non-200 status code(" + response.getStatusLine().getStatusCode() + ") reason:"
                      + response.getStatusLine().getReasonPhrase());
          }
          String entity = EntityUtils.toString(response.getEntity());
          response.close();
          return entity;
      } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | IOException e) {
          throw new IOException(e);
      }
  }

  public static String getHostNameBasedOnConfig(LogStream logStream,
                                                     SingerConfig singerConfig) {
    if (singerConfig.isKubernetesEnabled()) {
      if (logStream.getSingerLog().getPodUid() !=null 
          && logStream.getSingerLog().getPodUid() != LogStreamManager.NON_KUBERNETES_POD_ID) {
        return logStream.getSingerLog().getPodUid();
      }
    }
    return SingerUtils.getHostname();
  }

  public static void deleteRecursively(File baseDir) {
    if (baseDir!=null && baseDir.listFiles()!=null) {
      for (File file : baseDir.listFiles()) {
        if (file.isDirectory()) {
          deleteRecursively(file);
        }
        file.delete();
      }
    }
  }
  
}
