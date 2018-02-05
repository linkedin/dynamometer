/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer;

import org.apache.hadoop.hdfs.server.datanode.DataNodeLayoutVersion;


/**
 * Simple utility class to print out the current DataNode layout version
 */
public class DataNodeLayoutVersionFetcher {

  public static void main(String[] args) {
    System.out.printf("%d%n", DataNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
  }

}
