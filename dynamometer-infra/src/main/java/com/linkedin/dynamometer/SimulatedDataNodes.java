/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Starts up a number of DataNodes within the same JVM. These DataNodes all use
 * {@link org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset}, so they do not store any actual data, and do not
 * persist anything to disk; they maintain all metadata in memory. This is useful for testing and simulation purposes.
 * <p>
 * The DataNodes will attempt to connect to a NameNode defined by the default FileSystem. There will be one DataNode
 * started for each block list file passed as an argument. Each of these files should contain a list of blocks that
 * the corresponding DataNode should contain, as specified by a triplet of block ID, block size, and generation stamp.
 * Each line of the file is one block, in the format:
 * <p>
 * {@code blockID,blockGenStamp,blockSize}
 * <p>
 * This class is loosely based off of {@link org.apache.hadoop.hdfs.DataNodeCluster}.
 */
public class SimulatedDataNodes extends Configured implements Tool {

  // Set this arbitrarily large (100TB) since we don't care about storage capacity
  private static final long STORAGE_CAPACITY = 100 * 2L<<40;
  private static final String USAGE =
      "Usage: com.linkedin.dynamometer.SimulatedDataNodes bpid blockListFile1 [ blockListFileN ... ]\n" +
          "   bpid should be the ID of the block pool to which these DataNodes belong.\n" +
          "   Each blockListFile specified should contain a list of blocks to be served by one DataNode.\n" +
          "   See the Javadoc of this class for more detail.";

  static void printUsageExit(String err) {
    System.out.println(err);
    System.out.println(USAGE);
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    SimulatedDataNodes datanodes = new SimulatedDataNodes();
    ToolRunner.run(new HdfsConfiguration(), datanodes, args);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      printUsageExit("Not enough arguments");
    }
    String bpid = args[0];
    List<Path> blockListFiles = new ArrayList<>();
    for (int i = 1; i < args.length; i++) {
      blockListFiles.add(new Path(args[i]));
    }

    URI defaultFS = FileSystem.getDefaultUri(getConf());
    if (!HdfsConstants.HDFS_URI_SCHEME.equals(defaultFS.getScheme())) {
      printUsageExit("Must specify an HDFS-based default FS! Got <" + defaultFS + ">");
    }
    String nameNodeAdr = defaultFS.getAuthority();
    if (nameNodeAdr == null) {
      printUsageExit("No NameNode address and port in config");
    }
    System.out.println("DataNodes will connect to NameNode at " + nameNodeAdr);

    System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA,
        DataNode.getStorageLocations(getConf()).get(0).getFile().getAbsolutePath());
    SimulatedMultiStorageFSDataset.setFactory(getConf());
    getConf().setLong(SimulatedMultiStorageFSDataset.CONFIG_PROPERTY_CAPACITY, STORAGE_CAPACITY);

    UserGroupInformation.setConfiguration(getConf());
    MiniDFSCluster mc = new MiniDFSCluster();
    try {
      mc.formatDataNodeDirs();
    } catch (IOException e) {
      System.out.println("Error formatting DataNode dirs: " + e);
      System.exit(1);
    }

    try {
      System.out.println("Found " + blockListFiles.size() + " block listing files; launching DataNodes accordingly.");
      mc.startDataNodes(getConf(), blockListFiles.size(), null, false, StartupOption.REGULAR,
          null, null, null, null, false, true, true, null);
      long startTime = Time.monotonicNow();
      System.out.println("Waiting for DataNodes to connect to NameNode and init storage directories.");
      Set<DataNode> datanodesWithoutFSDataset = new HashSet<>(mc.getDataNodes());
      while (!datanodesWithoutFSDataset.isEmpty()) {
        Iterator<DataNode> iter = datanodesWithoutFSDataset.iterator();
        while (iter.hasNext()) {
          if (DataNodeTestUtils.getFSDataset(iter.next()) != null) {
            iter.remove();
          }
        }
        Thread.sleep(100);
      }
      System.out.println("Waited " + (Time.monotonicNow() - startTime) + " ms for DataNode FSDatasets to be ready");

      for (int dnIndex = 0; dnIndex < blockListFiles.size(); dnIndex++) {
        Path blockListFile = blockListFiles.get(dnIndex);
        try (FSDataInputStream fsdis = blockListFile.getFileSystem(getConf()).open(blockListFile)) {
          BufferedReader reader = new BufferedReader(new InputStreamReader(fsdis));
          List<Block> blockList = new ArrayList<>();
          int cnt = 0;
          for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            // Format of the listing files is blockID,blockGenStamp,blockSize
            String[] blockInfo = line.split(",");
            blockList.add(new Block(Long.parseLong(blockInfo[0]),
                Long.parseLong(blockInfo[2]), Long.parseLong(blockInfo[1])));
            cnt++;
          }
          try {
            mc.injectBlocks(dnIndex, blockList, bpid);
          } catch (IOException ioe) {
            System.out.printf("Error injecting blocks into DataNode %d for block pool %s: %s%n", dnIndex, bpid,
                ExceptionUtils.getFullStackTrace(ioe));
          }
          System.out.printf("Injected %d blocks into DataNode %d for block pool %s%n", cnt, dnIndex, bpid);
        }
      }

    } catch (IOException e) {
      System.out.println("Error creating DataNodes: " + ExceptionUtils.getFullStackTrace(e));
      return 1;
    }
    return 0;
  }

}
