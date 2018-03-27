/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.linkedin.dynamometer.workloadgenerator.audit.AuditLogDirectParser;
import com.linkedin.dynamometer.workloadgenerator.audit.AuditReplayMapper;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
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
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.After;
import org.junit.AfterClass;
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
 * To run this test JAVA_HOME must be set correctly.
 * This also relies on the {@code tar} and {@code truncate} utilities
 * being available ({@code truncate} is generally required to run Dynamometer).
 *
 * You can optionally specify which version of HDFS should be started within
 * the Dynamometer cluster; the default is 2.7.5. This can be adjusted by setting
 * the {@value HADOOP_BIN_VERSION_KEY} property. This will automatically download
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

  private static final int MINICLUSTER_NUM_NMS = 1;
  private static final int MINICLUSTER_NUM_DNS = 1;

  private static final String HADOOP_BIN_PATH_KEY = "dyno.hadoop.bin.path";
  private static final String HADOOP_BIN_VERSION_KEY = "dyno.hadoop.bin.version";
  private static final String HADOOP_BIN_VERSION_DEFAULT = "2.7.5";
  private static final String FSIMAGE_FILENAME = "fsimage_0000000000000061740";
  private static final String VERSION_FILENAME = "VERSION";

  private static MiniDFSCluster miniDFSCluster;
  private static MiniYARNCluster miniYARNCluster;
  private static YarnClient yarnClient;
  private static FileSystem fs;
  private static Configuration conf;
  private static Configuration yarnConf;
  private static Path fsImageTmpPath;
  private static Path fsVersionTmpPath;
  private static Path blockImageOutputDir;
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
    Shell.ShellCommandExecutor truncateCheck =
        new Shell.ShellCommandExecutor(new String[] {"bash", "-c", "command -v truncate"});
    truncateCheck.execute();
    if (truncateCheck.getExitCode() != 0) {
      fail("truncate command is not available");
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

    conf.setBoolean("yarn.minicluster.fixed.ports", true);
    conf.setBoolean("yarn.minicluster.use-rpc", true);
    conf.setInt("yarn.scheduler.minimum-allocation-mb", 128);
    // This is necessary to have the RM respect our vcore allocation request
    conf.set("yarn.scheduler.capacity.resource-calculator",
        "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator");
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

    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(new Configuration(yarnConf));
    yarnClient.start();

    fsImageTmpPath = fs.makeQualified(new Path("/tmp/" + FSIMAGE_FILENAME));
    fsVersionTmpPath = fs.makeQualified(new Path("/tmp/" + VERSION_FILENAME));
    blockImageOutputDir = fs.makeQualified(new Path("/tmp/blocks"));

    uploadFsimageResourcesToHDFS(hadoopBinVersion);

    miniYARNCluster.waitForNodeManagersToConnect(30000);
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
              "-" + Client.CONF_PATH_ARG, getResourcePath("conf").toString(),
              "-" + Client.BLOCK_LIST_PATH_ARG, blockImageOutputDir.toString(),
              "-" + Client.FS_IMAGE_DIR_ARG, fsImageTmpPath.getParent().toString(),
              "-" + Client.HADOOP_BINARY_PATH_ARG, hadoopTarballPath.getAbsolutePath(),
              "-" + AMOptions.DATANODES_PER_CLUSTER_ARG, "2",
              "-" + AMOptions.DATANODE_MEMORY_MB_ARG, "128",
              "-" + AMOptions.NAMENODE_MEMORY_MB_ARG, "256",
              "-" + AMOptions.NAMENODE_METRICS_PERIOD_ARG, "1",
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

    DynoInfraUtils.waitForNameNodeReadiness(namenodeProperties.get(), 3, falseSupplier, LOG);

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
    assertEquals(3, counters.findCounter(AuditReplayMapper.REPLAYCOUNTERS.TOTALCOMMANDS).getValue());
    assertEquals(0, counters.findCounter(AuditReplayMapper.REPLAYCOUNTERS.TOTALINVALIDCOMMANDS).getValue());

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
    fs.copyFromLocalFile(new Path(getResourcePath("blocks")), new Path("/tmp/"));
    fs.copyFromLocalFile(new Path(getResourcePath("audit_trace_direct")), new Path("/tmp/"));
  }

}
