/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.blockgenerator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestBlockGen {
  private static final Log LOG = LogFactory.getLog(TestBlockGen.class);

  private MiniDFSCluster dfsCluster;
  private static final Configuration conf = new Configuration();
  private FileSystem fs;
  private static final String fsImageName = "fsimage_0000000000000061740.xml";
  private static final String blockListOutputDirName = "blockLists";
  private Path tmpPath;

  @Before
  public void setup() throws Exception {
    dfsCluster = new MiniDFSCluster.Builder(conf).build();
    dfsCluster.waitActive();
    LOG.info("Started MiniDFSCluster");
    fs = dfsCluster.getFileSystem();
    FileSystem.setDefaultUri(conf, fs.getUri());
    tmpPath = fs.makeQualified(new Path("/tmp"));
    fs.mkdirs(tmpPath);
    String fsImageFile = this.getClass().getClassLoader().getResource(fsImageName).getPath();

    fs.copyFromLocalFile(new Path(fsImageFile), new Path(tmpPath, fsImageName));
  }

  @After
  public void cleanUp() throws Exception {
    dfsCluster.shutdown();
  }

  @Test
  public void testBlockGen() throws Exception {
    LOG.info("Started test");

    int datanodeCount = 40;

    GenerateBlockImagesDriver driver = new GenerateBlockImagesDriver(new Configuration());
    driver.run(new String[]{
        "-" + GenerateBlockImagesDriver.FSIMAGE_INPUT_PATH_ARG, new Path(tmpPath, fsImageName).toString(),
        "-" + GenerateBlockImagesDriver.BLOCK_IMAGE_OUTPUT_ARG, new Path(tmpPath, blockListOutputDirName).toString(),
        "-" + GenerateBlockImagesDriver.NUM_DATANODES_ARG, String.valueOf(datanodeCount)});

    for (int i = 0; i < datanodeCount; i++) {
      final int idx = i;
      assertEquals(1, fs.listStatus(new Path(tmpPath, blockListOutputDirName), new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return path.getName().startsWith(String.format("dn%d-", idx));
        }
      }).length);
    }
  }
}
