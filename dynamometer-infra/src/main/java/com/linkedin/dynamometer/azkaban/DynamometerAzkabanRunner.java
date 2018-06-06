/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.azkaban;

import com.linkedin.dynamometer.ApplicationMaster;
import com.linkedin.dynamometer.Client;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;


/**
 * A class used to be able to run Dynamometer via Azkaban. It is expected
 * to be run as a HadoopJavaJob type. Any Azkaban property prefixed with
 * `{@code dyno.}` will be used as an argument to the Dynamometer infrastructure
 * client. For example, setting the property `{@code dyno.foo}` to `{@code bar}`
 * would result in the client receiving the arguments `{@code -foo bar}`.
 * For arguments which do not expect an argument, specify them as
 * `{@code dyno.flag.foo}`; setting a value of true will include the flag
 * `{@code -foo}` and setting a value of false will do nothing.
 */
public class DynamometerAzkabanRunner extends Configured {

  public static final String DYNO_PROPERTY_PREFIX = "dyno.";
  public static final String DYNO_FLAG_PREFIX = "flag.";

  private static final Log LOG = LogFactory.getLog(DynamometerAzkabanRunner.class);
  private final String name;
  private final Properties properties;

  private Client dynoClient;

  /**
   * The constructor expected by Azkaban.
   * @param name The name of the application.
   * @param properties The properties to be used by this application.
   */
  public DynamometerAzkabanRunner(String name, Properties properties) {
    super(new YarnConfiguration());
    this.name = name;
    this.properties = properties;
  }

  /**
   * Expected by Azkaban; this is the main entrypoint.
   */
  public void run() throws Exception {
    dynoClient = new Client(ClassUtil.findContainingJar(ApplicationMaster.class));
    dynoClient.setConf(getConf());

    List<String> argList = new ArrayList<>();
    argList.add("-" + Client.APPNAME_ARG);
    argList.add(name);
    String tokenFileLocation = System.getenv("HADOOP_TOKEN_FILE_LOCATION");
    if (tokenFileLocation != null) {
      argList.add("-" + Client.TOKEN_FILE_LOCATION_ARG);
      argList.add(tokenFileLocation);
    }
    for (Map.Entry<Object, Object> property : properties.entrySet()) {
      String fullKey = (String) property.getKey();
      if (!fullKey.startsWith(DYNO_PROPERTY_PREFIX)) {
        continue;
      }
      String key = fullKey.substring(DYNO_PROPERTY_PREFIX.length());
      if (key.startsWith(DYNO_FLAG_PREFIX)) {
        String flag = key.substring(DYNO_FLAG_PREFIX.length());
        if (Boolean.valueOf((String) property.getValue())) {
          argList.add("-" + flag);
        }
        continue;
      }
      argList.add("-" + key);
      argList.add((String) property.getValue());
    }

    int returnCode = dynoClient.run(argList.toArray(new String[0]));
    if (returnCode != 0) {
      String message = "Dynamometer failed with return code: " + returnCode;
      LOG.error(message);
      throw new RuntimeException(message);
    }
  }

  /**
   * Expected by Azkaban to provide a way to kill the job.
   */
  public void cancel() throws Exception {
    if (dynoClient == null) {
      LOG.info("No Dynamometer client was found; exiting without any action.");
    } else {
      dynoClient.attemptCleanup();
    }
  }

}
