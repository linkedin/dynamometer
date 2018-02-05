/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;


/**
 * A collection of utilities used by the Dynamometer infrastructure application.
 */
public class DynoInfraUtils {

  public static final String APACHE_DOWNLOAD_MIRROR_KEY = "dyno.apache-mirror";
  // Set a generic mirror as the default.
  public static final String APACHE_DOWNLOAD_MIRROR_DEFAULT = "http://mirrors.ocf.berkeley.edu/apache/";
  private static final String APACHE_DOWNLOAD_MIRROR_SUFFIX_FORMAT = "hadoop/common/hadoop-%s/hadoop-%s.tar.gz";
  public static final String HADOOP_TAR_FILENAME_FORMAT = "hadoop-%s.tar.gz";

  // The JMX bean queries to execute for various beans.
  public static final String NAMENODE_STARTUP_PROGRESS_JMX_QUERY = "Hadoop:service=NameNode,name=StartupProgress";
  public static final String FSNAMESYSTEM_JMX_QUERY = "Hadoop:service=NameNode,name=FSNamesystem";
  public static final String FSNAMESYSTEM_STATE_JMX_QUERY = "Hadoop:service=NameNode,name=FSNamesystemState";
  // The JMX property names of various properties.
  public static final String JMX_MISSING_BLOCKS = "MissingBlocks";
  public static final String JMX_UNDER_REPLICATED_BLOCKS = "UnderReplicatedBlocks";
  public static final String JMX_BLOCKS_TOTAL = "BlocksTotal";
  public static final String JMX_LIVE_NODE_COUNT = "NumLiveDataNodes";

  /**
   * If a file matching {@value HADOOP_TAR_FILENAME_FORMAT} and {@code version} is
   * found in {@code destinationDir}, return its path. Otherwise, first download the tarball
   * from an Apache mirror. If the {@value APACHE_DOWNLOAD_MIRROR_KEY} configuration or system
   * property (checked in that order) is set, use that as the mirror; else use
   * {@value APACHE_DOWNLOAD_MIRROR_DEFAULT}.
   * @param version The version of Hadoop to download, like "2.7.4" or "3.0.0-beta1"
   * @return The path to the tarball.
   */
  public static File fetchHadoopTarball(File destinationDir, String version, Configuration conf, Log log)
      throws IOException {
    log.info("Looking for Hadoop tarball for version: " + version);
    File destinationFile = new File(destinationDir, String.format(HADOOP_TAR_FILENAME_FORMAT, version));
    if (destinationFile.exists()) {
      log.info("Found tarball at: " + destinationFile.getAbsolutePath());
      return destinationFile;
    }
    String apacheMirror = conf.get(APACHE_DOWNLOAD_MIRROR_KEY);
    if (apacheMirror == null) {
      apacheMirror = System.getProperty(APACHE_DOWNLOAD_MIRROR_KEY, APACHE_DOWNLOAD_MIRROR_DEFAULT);
    }

    destinationDir.mkdirs();
    URL downloadURL = new URL(apacheMirror + String.format(APACHE_DOWNLOAD_MIRROR_SUFFIX_FORMAT, version, version));
    log.info("Downloading tarball from: <" + downloadURL + "> to <" + destinationFile.getAbsolutePath() + ">");
    FileUtils.copyURLToFile(downloadURL, destinationFile, 10000, 60000);
    log.info("Completed downloading of Hadoop tarball");
    return destinationFile;
  }

  /**
   * Get the URI that can be used to access the launched NameNode for HDFS RPCs.
   * @param nameNodeProperties The set of properties representing the information about the launched NameNode.
   * @return The HDFS URI.
   */
  static URI getNameNodeHdfsUri(Properties nameNodeProperties) {
    return URI.create(String.format("hdfs://%s:%s/", nameNodeProperties.getProperty(DynoConstants.NN_HOSTNAME),
        nameNodeProperties.getProperty(DynoConstants.NN_RPC_PORT)));
  }

  /**
   * Get the URI that can be used to access the launched NameNode for HDFS Service RPCs (i.e. from DataNodes).
   * @param nameNodeProperties The set of properties representing the information about the launched NameNode.
   * @return The service RPC URI.
   */
  static URI getNameNodeServiceRpcAddr(Properties nameNodeProperties) {
    return URI.create(String.format("hdfs://%s:%s/", nameNodeProperties.getProperty(DynoConstants.NN_HOSTNAME),
        nameNodeProperties.getProperty(DynoConstants.NN_SERVICERPC_PORT)));
  }

  /**
   * Get the URI that can be used to access the launched NameNode's web UI, e.g. for JMX calls.
   * @param nameNodeProperties The set of properties representing the information about the launched NameNode.
   * @return The URI to the web UI.
   */
  static URI getNameNodeWebUri(Properties nameNodeProperties) {
    return URI.create(String.format("http://%s:%s/", nameNodeProperties.getProperty(DynoConstants.NN_HOSTNAME),
      nameNodeProperties.getProperty(DynoConstants.NN_HTTP_PORT)));
  }

  /**
   * Get the URI that can be used to access the tracking interface for the NameNode, i.e. the web UI of the
   * NodeManager hosting the NameNode container.
   * @param nameNodeProperties The set of properties representing the information about the launched NameNode.
   * @return The tracking URI.
   */
  static URI getNameNodeTrackingUri(Properties nameNodeProperties) throws IOException {
    return URI.create(String.format("http://%s:%s/node/containerlogs/%s/%s/",
        nameNodeProperties.getProperty(DynoConstants.NN_HOSTNAME),
        nameNodeProperties.getProperty(Environment.NM_HTTP_PORT.name()),
        nameNodeProperties.getProperty(Environment.CONTAINER_ID.name()),
        UserGroupInformation.getCurrentUser().getShortUserName()));
  }

  /**
   * Get the set of properties representing information about the launched NameNode. This method will wait for
   * the information to be available until it is interrupted, or {@code shouldExit} returns true. It polls for
   * a file present at {@code nameNodeInfoPath} once a second and uses that file to load the NameNode information.
   * @param shouldExit Should return true iff this should stop waiting.
   * @param conf The configuration.
   * @param nameNodeInfoPath The path at which to expect the NameNode information file to be present.
   * @param log Where to log information.
   * @return Absent if this exited prematurely (i.e. due to {@code shouldExit}), else returns a set of properties
   *         representing information about the launched NameNode.
   */
  static Optional<Properties> waitForAndGetNameNodeProperties(Supplier<Boolean> shouldExit,
      Configuration conf, Path nameNodeInfoPath, Log log) throws IOException, InterruptedException {
    while (!shouldExit.get()) {
      try (FSDataInputStream nnInfoInputStream = nameNodeInfoPath.getFileSystem(conf).open(nameNodeInfoPath)) {
        Properties nameNodeProperties = new Properties();
        nameNodeProperties.load(nnInfoInputStream);
        return Optional.of(nameNodeProperties);
      } catch (FileNotFoundException fnfe) {
        log.debug("NameNode host information not yet available");
        Thread.sleep(1000);
      } catch (IOException ioe) {
        log.warn("Unable to fetch NameNode host information; retrying", ioe);
        Thread.sleep(1000);
      }
    }
    return Optional.absent();
  }

  /**
   * Wait for the launched NameNode to finish starting up. Continues until {@code shouldExit} returns true.
   * @param nameNodeProperties The set of properties containing information about the NameNode.
   * @param shouldExit Should return true iff this should stop waiting.
   * @param log Where to log information.
   */
  static void waitForNameNodeStartup(Properties nameNodeProperties, Supplier<Boolean> shouldExit, Log log)
      throws IOException, InterruptedException {
    if (shouldExit.get()) {
      return;
    }
    log.info("Waiting for NameNode to finish starting up...");
    waitForNameNodeJMXValue("Startup progress", NAMENODE_STARTUP_PROGRESS_JMX_QUERY,
        "PercentComplete", 1.0, 0.01, false, nameNodeProperties, shouldExit, log);
    log.info("NameNode has started!");
  }

  /**
   * Wait for the launched NameNode to be ready, i.e. to have at least 99% of its DataNodes register, have fewer
   * than 0.01% of its blocks missing, and less than 1% of its blocks under replicated. Continues until the
   * criteria have been met or {@code shouldExit} returns true.
   * @param nameNodeProperties The set of properties containing information about the NameNode.
   * @param numTotalDataNodes Total expected number of DataNodes to register.
   * @param shouldExit Should return true iff this should stop waiting.
   * @param log Where to log inormation.
   */
  static void waitForNameNodeReadiness(final Properties nameNodeProperties, int numTotalDataNodes,
      Supplier<Boolean> shouldExit, final Log log) throws IOException, InterruptedException {
    if (shouldExit.get()) {
      return;
    }
    log.info(String.format("Waiting for %d DataNodes to register with the NameNode...",
        (int) (numTotalDataNodes*0.99f)));
    waitForNameNodeJMXValue("Number of live DataNodes", FSNAMESYSTEM_STATE_JMX_QUERY, JMX_LIVE_NODE_COUNT,
        numTotalDataNodes*0.99, numTotalDataNodes*0.001, false, nameNodeProperties, shouldExit, log);
    final int totalBlocks = Integer.parseInt(fetchNameNodeJMXValue(nameNodeProperties, FSNAMESYSTEM_STATE_JMX_QUERY,
        JMX_BLOCKS_TOTAL));
    log.info("Waiting for MissingBlocks to fall below " + totalBlocks*0.0001 + "...");
    waitForNameNodeJMXValue("Number of missing blocks", FSNAMESYSTEM_JMX_QUERY, JMX_MISSING_BLOCKS,
        totalBlocks*0.0001, totalBlocks*0.0001, true, nameNodeProperties, shouldExit, log);
    log.info("Waiting for UnderReplicatedBlocks to fall below " + totalBlocks*0.01 + "...");
    waitForNameNodeJMXValue("Number of under replicated blocks", FSNAMESYSTEM_STATE_JMX_QUERY,
        JMX_UNDER_REPLICATED_BLOCKS, totalBlocks*0.01, totalBlocks*0.001, true, nameNodeProperties, shouldExit, log);
    log.info("NameNode is ready for use!");
  }

  /**
   * Poll the launched NameNode's JMX for a specific value, waiting for it to cross some threshold. Continues until
   * the threshold has been crossed or {@code shouldExit} returns true. Periodically logs the current value.
   * @param valueName The human-readable name of the value which is being polled (for printing purposes only).
   * @param jmxBeanQuery The JMX bean query to execute; should return a JMX property matching {@code jmxProperty}.
   * @param jmxProperty The name of the JMX property whose value should be polled.
   * @param threshold The threshold value to wait for the JMX property to be above/below.
   * @param printThreshold The threshold between each log statement; controls how frequently the value is printed.
   *                       For example, if this was 10, a statement would be logged every time the value has changed
   *                       by more than 10.
   * @param decreasing True iff the property's value is decreasing and this should wait until it is lower than
   *                   threshold; else the value is treated as increasing and will wait until it is higher than
   *                   threshold.
   * @param nameNodeProperties The set of properties containing information about the NameNode.
   * @param shouldExit Should return true iff this should stop waiting.
   * @param log Where to log information.
   */
  private static void waitForNameNodeJMXValue(String valueName, String jmxBeanQuery, String jmxProperty,
      double threshold, double printThreshold, boolean decreasing, Properties nameNodeProperties,
      Supplier<Boolean> shouldExit, Log log) throws InterruptedException {
    double lastPrintedValue = decreasing ? Double.MAX_VALUE : Double.MIN_VALUE;
    double value;
    int retryCount = 0;
    long startTime = Time.monotonicNow();
    while (!shouldExit.get()) {
      try {
        value = Double.parseDouble(fetchNameNodeJMXValue(nameNodeProperties, jmxBeanQuery, jmxProperty));
        if ((decreasing && value <= threshold) || (!decreasing && value >= threshold)) {
          log.info(String.format("%s = %.2f; %s threshold of %.2f; done waiting after %d ms.", valueName, value,
              decreasing ? "below" : "above", threshold, Time.monotonicNow() - startTime));
          break;
        } else if (Math.abs(value - lastPrintedValue) >= printThreshold) {
          log.info(String.format("%s: %.2f", valueName, value));
          lastPrintedValue = value;
        }
      } catch (IOException ioe) {
        if (++retryCount % 20 == 0) {
          log.warn(String.format("Unable to fetch %s; retried %d times / waited %d ms: %s",
              valueName, retryCount, Time.monotonicNow() - startTime, ioe));
        }
      }
      Thread.sleep(3000);
    }
  }

  /**
   * Fetch a value from the launched NameNode's JMX.
   * @param nameNodeProperties The set of properties containing information about the NameNode.
   * @param jmxBeanQuery The JMX bean query to execute; should return a JMX property matching {@code jmxProperty}.
   * @param property The name of the JMX property whose value should be polled.
   * @return The value associated with the property.
   */
  static String fetchNameNodeJMXValue(Properties nameNodeProperties, String jmxBeanQuery, String property)
      throws IOException {
    URI nnWebUri = getNameNodeWebUri(nameNodeProperties);
    URL queryURL;
    try {
      queryURL = new URL(nnWebUri.getScheme(), nnWebUri.getHost(), nnWebUri.getPort(), "/jmx?qry=" + jmxBeanQuery);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(
          "Invalid JMX query: \"" + jmxBeanQuery + "\" against " + "NameNode URI: " + nnWebUri);
    }
    HttpURLConnection conn = (HttpURLConnection) queryURL.openConnection();
    if (conn.getResponseCode() != 200) {
      throw new IOException("Unable to retrieve JMX: " + conn.getResponseMessage());
    }
    InputStream in = conn.getInputStream();
    JsonFactory fac = new JsonFactory();
    JsonParser parser = fac.createJsonParser(in);
    if (parser.nextToken() != JsonToken.START_OBJECT ||
        parser.nextToken() != JsonToken.FIELD_NAME ||
        !parser.getCurrentName().equals("beans") ||
        parser.nextToken() != JsonToken.START_ARRAY ||
        parser.nextToken() != JsonToken.START_OBJECT) {
      throw new IOException("Unexpected format of JMX JSON response for: " + jmxBeanQuery);
    }
    int objectDepth = 1;
    String ret = null;
    while (objectDepth > 0) {
      JsonToken tok = parser.nextToken();
      if (tok == JsonToken.START_OBJECT) {
        objectDepth++;
      } else if (tok == JsonToken.END_OBJECT) {
        objectDepth--;
      } else if (tok == JsonToken.FIELD_NAME) {
        if (parser.getCurrentName().equals(property)) {
          parser.nextToken();
          ret = parser.getText();
          break;
        }
      }
    }
    parser.close();
    in.close();
    conn.disconnect();
    if (ret == null) {
      throw new IOException("Property " + property + " not found within " + jmxBeanQuery);
    } else {
      return ret;
    }
  }

}
