/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.Sets;
import com.linkedin.dynamometer.workloadgenerator.audit.AuditLogDirectParser;
import com.linkedin.dynamometer.workloadgenerator.audit.AuditReplayMapper;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.linkedin.dynamometer.DynoInfraUtils.fetchHadoopTarball;
import static org.apache.hadoop.hdfs.MiniDFSCluster.PROP_TEST_BUILD_DATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Start a Dynamometer cluster in a MiniYARNCluster. Ensure that the NameNode
 * is able to start correctly, exit safemode, and run some commands. Subsequently
 * the workload job is launched and it is verified that it completes successfully
 * and is able to replay commands as expected.
 *
 * To run this test JAVA_HOME must be set correctly, and the {@code tar} utility must be available.
 *
 * You can optionally specify which version of HDFS should be started within
 * the Dynamometer cluster; the default is {@value HADOOP_BIN_VERSION_DEFAULT}. This can be adjusted
 * by setting the {@value HADOOP_BIN_VERSION_KEY} property. This will automatically download
 * the correct Hadoop tarball for the specified version. It downloads from an Apache
 * mirror (by default {@value DynoInfraUtils#APACHE_DOWNLOAD_MIRROR_DEFAULT}); which mirror
 * is used can be controlled with the {@value DynoInfraUtils#APACHE_DOWNLOAD_MIRROR_KEY}
 * property. Note that mirrors normally contain only the latest releases on any given
 * release line; you may need to use {@code http://archive.apache.org/dist/} for older
 * releases. The downloaded tarball will be stored in the test directory and can be
 * reused between test executions. Alternatively, you can specify the
 * {@value HADOOP_BIN_PATH_KEY} property to point directly to a Hadoop tarball which
 * is present locally and no download will occur.
 */
public class TestDynamometerInfra {

  private static final Log LOG = LogFactory.getLog(TestDynamometerInfra.class);

  private static final int MINICLUSTER_NUM_NMS = 3;
  private static final int MINICLUSTER_NUM_DNS = 1;

  private static final String HADOOP_BIN_PATH_KEY = "dyno.hadoop.bin.path";
  private static final String HADOOP_BIN_VERSION_KEY = "dyno.hadoop.bin.version";
  private static final String HADOOP_BIN_VERSION_DEFAULT = "2.7.6";
  private static final String FSIMAGE_FILENAME = "fsimage_0000000000000061740";
  private static final String VERSION_FILENAME = "VERSION";

  private static final String NAMENODE_NODELABEL = "dyno_namenode";
  private static final String DATANODE_NODELABEL = "dyno_datanode";

  private static MiniDFSCluster miniDFSCluster;
  private static MiniYARNCluster miniYARNCluster;
  private static YarnClient yarnClient;
  private static FileSystem fs;
  private static Configuration conf;
  private static Configuration yarnConf;
  private static Path fsImageTmpPath;
  private static Path fsVersionTmpPath;
  private static Path blockImageOutputDir;
  private static Path auditTraceDir;
  private static Path confZip;
  private static File testBaseDir;
  private static File hadoopTarballPath;
  private static File hadoopUnpackedDir;

  private ApplicationId infraAppId;

  @BeforeClass
  public static void setupClass() throws Exception {
    if (System.getenv("JAVA_HOME") == null) {
      fail("JAVA_HOME must be set properly");
    }
    Shell.ShellCommandExecutor tarCheck =
        new Shell.ShellCommandExecutor(new String[] {"bash", "-c", "command -v tar"});
    tarCheck.execute();
    if (tarCheck.getExitCode() != 0) {
      fail("tar command is not available");
    }

    conf = new Configuration();
    // Follow the conventions of MiniDFSCluster
    testBaseDir = new File(System.getProperty(PROP_TEST_BUILD_DATA, "build/test/data"));
    String hadoopBinVersion = System.getProperty(HADOOP_BIN_VERSION_KEY, HADOOP_BIN_VERSION_DEFAULT);
    if (System.getProperty(HADOOP_BIN_PATH_KEY) == null) {
      hadoopTarballPath = fetchHadoopTarball(testBaseDir, hadoopBinVersion, conf, LOG);
    } else {
      hadoopTarballPath = new File(System.getProperty(HADOOP_BIN_PATH_KEY));
    }
    // Set up the Hadoop binary to be used as the system-level Hadoop install
    hadoopUnpackedDir = new File(testBaseDir, "hadoop" + UUID.randomUUID());
    assertTrue("Failed to make temporary directory", hadoopUnpackedDir.mkdirs());
    Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(new String[] { "tar", "xzf",
        hadoopTarballPath.getAbsolutePath(), "-C", hadoopUnpackedDir.getAbsolutePath() });
    shexec.execute();
    if (shexec.getExitCode() != 0) {
      fail("Unable to execute tar to expand Hadoop binary");
    }

    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    for (String q : new String[] { "root", "root.default" } ) {
      conf.setInt(CapacitySchedulerConfiguration.PREFIX + q + "." + CapacitySchedulerConfiguration.CAPACITY, 100);
      String accessibleNodeLabelPrefix = CapacitySchedulerConfiguration.PREFIX + q + "." +
          CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS;
      conf.set(accessibleNodeLabelPrefix, CapacitySchedulerConfiguration.ALL_ACL);
      conf.setInt(
          accessibleNodeLabelPrefix + "." + DATANODE_NODELABEL + "." + CapacitySchedulerConfiguration.CAPACITY, 100);
      conf.setInt(
          accessibleNodeLabelPrefix + "." + NAMENODE_NODELABEL + "." + CapacitySchedulerConfiguration.CAPACITY, 100);
    }
    // This is necessary to have the RM respect our vcore allocation request
    conf.setClass(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class, ResourceCalculator.class);
    miniYARNCluster = new MiniYARNCluster(
        TestDynamometerInfra.class.getName(), 1, MINICLUSTER_NUM_NMS, 1, 1);
    miniYARNCluster.init(conf);
    miniYARNCluster.start();

    yarnConf = miniYARNCluster.getConfig();
    miniDFSCluster = new MiniDFSCluster.Builder(conf)
        .format(true).numDataNodes(MINICLUSTER_NUM_DNS).build();
    miniDFSCluster.waitClusterUp();
    FileSystem.setDefaultUri(conf, miniDFSCluster.getURI());
    FileSystem.setDefaultUri(yarnConf, miniDFSCluster.getURI());
    fs = miniDFSCluster.getFileSystem();

    URL url = Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml");
    if (url == null) {
      throw new RuntimeException("Could not find 'yarn-site.xml' dummy file in classpath");
    }
    yarnConf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, new File(url.getPath()).getParent());
    // Write the XML to a buffer before writing to the file. writeXml() can trigger a read of the existing
    // yarn-site.xml, so writing directly could trigger a read of the file while it is in an inconsistent state
    // (partially written)
    try (ByteArrayOutputStream bytesOut = new ByteArrayOutputStream()) {
      yarnConf.writeXml(bytesOut);
      try (OutputStream fileOut = new FileOutputStream(new File(url.getPath()))) {
        fileOut.write(bytesOut.toByteArray());
      }
    }

    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(new Configuration(yarnConf));
    yarnClient.start();

    fsImageTmpPath = fs.makeQualified(new Path("/tmp/" + FSIMAGE_FILENAME));
    fsVersionTmpPath = fs.makeQualified(new Path("/tmp/" + VERSION_FILENAME));
    blockImageOutputDir = fs.makeQualified(new Path("/tmp/blocks"));
    auditTraceDir = fs.makeQualified(new Path("/tmp/audit_trace_direct"));
    confZip = fs.makeQualified(new Path("/tmp/conf.zip"));

    uploadFsimageResourcesToHDFS(hadoopBinVersion);

    miniYARNCluster.waitForNodeManagersToConnect(30000);

    RMNodeLabelsManager nodeLabelManager = miniYARNCluster.getResourceManager().getRMContext().getNodeLabelManager();
    nodeLabelManager.addToCluserNodeLabels(Sets.newHashSet(NAMENODE_NODELABEL, DATANODE_NODELABEL));
    Map<NodeId, Set<String>> nodeLabels = new HashMap<>();
    nodeLabels.put(miniYARNCluster.getNodeManager(0).getNMContext().getNodeId(), Sets.newHashSet(NAMENODE_NODELABEL));
    nodeLabels.put(miniYARNCluster.getNodeManager(1).getNMContext().getNodeId(), Sets.newHashSet(DATANODE_NODELABEL));
    nodeLabelManager.addLabelsToNode(nodeLabels);
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    if (miniDFSCluster != null) {
      miniDFSCluster.shutdown(true);
      miniDFSCluster = null;
    }
    if (yarnClient != null) {
      yarnClient.stop();
      yarnClient = null;
    }
    if (miniYARNCluster != null) {
      miniYARNCluster.getResourceManager().stop();
      miniYARNCluster.getResourceManager().waitForServiceToStop(30000);
      miniYARNCluster.stop();
      miniYARNCluster.waitForServiceToStop(30000);
      FileUtils.deleteDirectory(miniYARNCluster.getTestWorkDir());
      miniYARNCluster = null;
    }
    if (hadoopUnpackedDir != null) {
      FileUtils.deleteDirectory(hadoopUnpackedDir);
    }
  }

  @After
  public void tearDown() throws Exception {
    if (infraAppId != null && yarnClient != null) {
      yarnClient.killApplication(infraAppId);
    }
    infraAppId = null;
  }

  @Test
  public void testNameNodeInYARN() throws Exception {
    final Client client = new Client(JarFinder.getJar(ApplicationMaster.class));
    Configuration conf = new Configuration(yarnConf);
    conf.setLong(AuditLogDirectParser.AUDIT_START_TIMESTAMP_KEY, 60000);
    client.setConf(conf);

    Thread appThread = new Thread() {
      @Override
      public void run() {
        try {
          client.run(new String[] {
              "-" + Client.MASTER_MEMORY_MB_ARG, "128",
              "-" + Client.CONF_PATH_ARG, confZip.toString(),
              "-" + Client.BLOCK_LIST_PATH_ARG, blockImageOutputDir.toString(),
              "-" + Client.FS_IMAGE_DIR_ARG, fsImageTmpPath.getParent().toString(),
              "-" + Client.HADOOP_BINARY_PATH_ARG, hadoopTarballPath.getAbsolutePath(),
              "-" + AMOptions.DATANODES_PER_CLUSTER_ARG, "2",
              "-" + AMOptions.DATANODE_MEMORY_MB_ARG, "128",
              "-" + AMOptions.DATANODE_NODELABEL_ARG, DATANODE_NODELABEL,
              "-" + AMOptions.NAMENODE_MEMORY_MB_ARG, "256",
              "-" + AMOptions.NAMENODE_METRICS_PERIOD_ARG, "1",
              "-" + AMOptions.NAMENODE_NODELABEL_ARG, NAMENODE_NODELABEL,
              "-" + AMOptions.SHELL_ENV_ARG, "HADOOP_HOME=" + getHadoopHomeLocation(),
              "-" + AMOptions.SHELL_ENV_ARG, "HADOOP_CONF_DIR=" + getHadoopHomeLocation() + "/etc/hadoop",
              "-" + Client.WORKLOAD_REPLAY_ENABLE_ARG,
              "-" + Client.WORKLOAD_INPUT_PATH_ARG, fs.makeQualified(new Path("/tmp/audit_trace_direct")).toString(),
              "-" + Client.WORKLOAD_THREADS_PER_MAPPER_ARG, "1",
              "-" + Client.WORKLOAD_START_DELAY_ARG, "10s",
              "-" + AMOptions.NAMENODE_ARGS_ARG, "-Ddfs.namenode.safemode.extension=0"
          });
        } catch (Exception e) {
          LOG.error("Error running client", e);
        }
      }
    };

    appThread.start();
    LOG.info("Waiting for application ID to become available");
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          List<ApplicationReport> apps = yarnClient.getApplications();
          if (apps.size() == 1) {
            infraAppId = apps.get(0).getApplicationId();
            return true;
          } else if (apps.size() > 1) {
            fail("Unexpected: more than one application");
          }
        } catch (IOException|YarnException e) {
          fail("Unexpected exception: " + e);
        }
        return false;
      }
    }, 1000, 60000);

    Supplier<Boolean> falseSupplier = new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return false;
      }
    };
    Optional<Properties> namenodeProperties = DynoInfraUtils.waitForAndGetNameNodeProperties(falseSupplier, conf,
        client.getNameNodeInfoPath(), LOG);
    if (!namenodeProperties.isPresent()) {
      fail("Unable to fetch NameNode properties");
    }

    DynoInfraUtils.waitForNameNodeReadiness(namenodeProperties.get(), 3, false, falseSupplier, conf, LOG);

    // Test that we can successfully write to / read from the cluster
    try {
      DistributedFileSystem dynoFS = (DistributedFileSystem)
          FileSystem.get(DynoInfraUtils.getNameNodeHdfsUri(namenodeProperties.get()), conf);
      Path testFile = new Path("/tmp/test/foo");
      dynoFS.mkdir(testFile.getParent(), FsPermission.getDefault());
      FSDataOutputStream out = dynoFS.create(testFile, (short) 1);
      out.write(42);
      out.hsync();
      out.close();
      FileStatus[] stats = dynoFS.listStatus(testFile.getParent());
      assertEquals(1, stats.length);
      assertEquals("foo", stats[0].getPath().getName());
    } catch (IOException e) {
      LOG.error("Failed to write or read", e);
      throw e;
    }

    Map<ContainerId, Container> namenodeContainers = miniYARNCluster.getNodeManager(0).getNMContext().getContainers();
    Map<ContainerId, Container> datanodeContainers = miniYARNCluster.getNodeManager(1).getNMContext().getContainers();
    Map<ContainerId, Container> amContainers = miniYARNCluster.getNodeManager(2).getNMContext().getContainers();
    assertEquals(1, namenodeContainers.size());
    assertEquals(2, namenodeContainers.keySet().iterator().next().getContainerId());
    assertEquals(2, datanodeContainers.size());
    assertEquals(1, amContainers.size());
    assertEquals(1, amContainers.keySet().iterator().next().getContainerId());

    LOG.info("Waiting for workload job to start and complete");
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          return client.getWorkloadJob() != null && client.getWorkloadJob().isComplete();
        } catch (IOException | IllegalStateException e) {
          return false;
        }
      }
    }, 3000, 60000);
    LOG.info("Workload job completed");

    if (!client.getWorkloadJob().isSuccessful()) {
      fail("Workload job failed");
    }
    Counters counters = client.getWorkloadJob().getCounters();
    assertEquals(6, counters.findCounter(AuditReplayMapper.REPLAYCOUNTERS.TOTALCOMMANDS).getValue());
    assertEquals(1, counters.findCounter(AuditReplayMapper.REPLAYCOUNTERS.TOTALINVALIDCOMMANDS).getValue());

    LOG.info("Waiting for infra application to exit");
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          ApplicationReport report = yarnClient.getApplicationReport(infraAppId);
          return report.getYarnApplicationState() == YarnApplicationState.KILLED;
        } catch (IOException | YarnException e) {
          return false;
        }
      }
    }, 3000, 300000);

    LOG.info("Waiting for metrics file to be ready");
    // Try to read the metrics file
    Path hdfsStoragePath = new Path(fs.getHomeDirectory(),
        DynoConstants.DYNAMOMETER_STORAGE_DIR + "/" + infraAppId);
    final Path metricsPath = new Path(hdfsStoragePath, "namenode_metrics");
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          FSDataInputStream in = fs.open(metricsPath);
          String metricsOutput = in.readUTF();
          in.close();
          // Just assert that there is some metrics content in there
          assertTrue(metricsOutput.contains("JvmMetrics"));
          return true;
        } catch (IOException ioe) {
          return false;
        }
      }
    }, 3000, 60000);
  }

  private static URI getResourcePath(String resourceName) {
    try {
      return TestDynamometerInfra.class.getClassLoader().getResource(resourceName).toURI();
    } catch (URISyntaxException e) {
      return null;
    }
  }

  /**
   * Get the Hadoop home location (i.e. for {@code HADOOP_HOME}) as the only
   * directory within the unpacked location of the Hadoop tarball.
   * @return The absolute path to the Hadoop home directory.
   */
  private String getHadoopHomeLocation() {
    File[] files = hadoopUnpackedDir.listFiles();
    if (files == null || files.length != 1) {
      fail("Should be 1 directory within the Hadoop unpacked dir");
    }
    return files[0].getAbsolutePath();
  }

  /**
   * Look for the resource files relevant to {@code hadoopBinVersion} and upload them
   * onto the MiniDFSCluster's HDFS for use by the subsequent jobs.
   * @param hadoopBinVersion The version string (e.g. "2.7.4") for which to look for resources.
   */
  private static void uploadFsimageResourcesToHDFS(String hadoopBinVersion) throws IOException {
    // Keep only the major/minor version for the resources path
    String[] versionComponents = hadoopBinVersion.split("\\.");
    if (versionComponents.length < 2) {
      fail("At least major and minor version are required to be specified; got: " + hadoopBinVersion);
    }
    String hadoopResourcesPath = "hadoop_" + versionComponents[0] + "_" + versionComponents[1];
    String fsImageResourcePath = hadoopResourcesPath + "/" + FSIMAGE_FILENAME;
    fs.copyFromLocalFile(new Path(getResourcePath(fsImageResourcePath)), fsImageTmpPath);
    fs.copyFromLocalFile(new Path(getResourcePath(fsImageResourcePath + ".md5")), fsImageTmpPath.suffix(".md5"));
    fs.copyFromLocalFile(new Path(getResourcePath(hadoopResourcesPath + "/" + VERSION_FILENAME)), fsVersionTmpPath);
    fs.mkdirs(auditTraceDir);
    IOUtils.copyBytes(TestDynamometerInfra.class.getClassLoader().getResourceAsStream("audit_trace_direct/audit0"),
        fs.create(new Path(auditTraceDir, "audit0")), conf, true);
    fs.mkdirs(blockImageOutputDir);
    for (String blockFile : new String[] { "dn0-a-0-r-00000", "dn1-a-0-r-00001", "dn2-a-0-r-00002" }) {
      IOUtils.copyBytes(TestDynamometerInfra.class.getClassLoader().getResourceAsStream("blocks/" + blockFile),
          fs.create(new Path(blockImageOutputDir, blockFile)), conf, true);
    }
    File tempConfZip = new File(testBaseDir, "conf.zip");
    ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(tempConfZip));
    for (String file : new String[] { "core-site.xml", "hdfs-site.xml", "log4j.properties" }) {
      zos.putNextEntry(new ZipEntry("etc/hadoop/" + file));
      InputStream is = TestDynamometerInfra.class.getClassLoader().getResourceAsStream("conf/etc/hadoop/" + file);
      IOUtils.copyBytes(is, zos, conf, false);
      is.close();
      zos.closeEntry();
    }
    zos.close();
    fs.copyFromLocalFile(new Path(tempConfZip.toURI()), confZip);
    tempConfZip.delete();
  }

}
