/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.hadoop.hdfs;

public class DynoDFSUtil {

  public static DFSClient getDFSClient(DistributedFileSystem dfs) {
    return dfs.dfs;
  }

}
