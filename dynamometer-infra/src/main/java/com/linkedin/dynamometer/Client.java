/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.linkedin.dynamometer.workloadgenerator.audit.AuditReplayMapper;
import com.linkedin.dynamometer.workloadgenerator.WorkloadDriver;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;


/**
 * Client for submitting a Dynamometer YARN application, and optionally, a workload MapReduce job.
 * This client uploads resources to HDFS as necessary for them to be accessed by the
 * YARN app, then launches an {@link ApplicationMaster}, which is responsible
 * for managing the lifetime of the application.
 *
 * The Dynamometer YARN application starts up the DataNodes of an HDFS
 * cluster. If the namenode_servicerpc_addr option is specified, it should
 * point to the service RPC address of an existing namenode, which the datanodes
 * will talk to. Else, a namenode will be launched internal to this YARN application.
 * The ApplicationMaster's logs contain links to the NN / DN containers to be able to
 * access their logs. Some of this information is also printed by the client.
 *
 * The application will store files in the submitting user's home directory
 * under a `.dynamometer/applicationID/` folder. This is mostly for uses
 * internal to the application, but if the NameNode is launched through YARN,
 * the NameNode's metrics will also be uploaded to a file `namenode_metrics`
 * within this folder. This file is also accessible as part of the NameNode's logs,
 * but this centralized location is easier to access for subsequent parsing.
 *
 * If the NameNode is launched internally, this Client will monitor the status of the NameNode, printing information
 * about its availability as the DataNodes register (e.g., outstanding under replicated blocks as block reports
 * arrive). If this is configured to launch the workload job, once the NameNode has gathered information from all of
 * its DataNodes, the client will launch a workload job which is configured to act against the newly launched
 * NameNode. Once the workload job completes, the infrastructure application will be shut down. At this time only
 * the audit log replay ({@link AuditReplayMapper}) workload is supported.
 *
 * If there is no workload job configured, this application will, by default, persist indefinitely until killed by
 * YARN. You can specify the timeout option to have it exit automatically after some time. This timeout will enforced
 * if there is a workload job configured as well.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Client extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(Client.class);

  public static final String APPNAME_ARG = "appname";
  public static final String APPNAME_DEFAULT = "DynamometerTest";
  public static final String QUEUE_ARG = "queue";
  public static final String QUEUE_DEFAULT = "default";
  public static final String TIMEOUT_ARG = "timeout";
  public static final String TIMEOUT_DEFAULT = "-1";
  public static final String HADOOP_VERSION_ARG = "hadoop_version";
  public static final String HADOOP_BINARY_PATH_ARG = "hadoop_binary_path";
  public static final String NAMENODE_SERVICERPC_ADDR_ARG = "namenode_servicerpc_addr";
  public static final String FS_IMAGE_DIR_ARG = "fs_image_dir";
  public static final String BLOCK_LIST_PATH_ARG = "block_list_path";
  public static final String CONF_PATH_ARG = "conf_path";
  public static final String MASTER_VCORES_ARG = "master_vcores";
  public static final String MASTER_VCORES_DEFAULT = "1";
  public static final String MASTER_MEMORY_MB_ARG = "master_memory_mb";
  public static final String MASTER_MEMORY_MB_DEFAULT = "2048";
  public static final String TOKEN_FILE_LOCATION_ARG = "token_file_location";
  public static final String WORKLOAD_REPLAY_ENABLE_ARG = "workload_replay_enable";
  public static final String WORKLOAD_INPUT_PATH_ARG = "workload_input_path";
  public static final String WORKLOAD_THREADS_PER_MAPPER_ARG = "workload_threads_per_mapper";
  public static final String WORKLOAD_START_DELAY_ARG = "workload_start_delay";
  public static final String WORKLOAD_RATE_FACTOR_ARG = "workload_rate_factor";
  public static final String WORKLOAD_RATE_FACTOR_DEFAULT = "1.0";
  public static final String WORKLOAD_CONFIG_ARG = "workload_config";

  private static final String[] ARCHIVE_FILE_TYPES = { ".zip", ".tar", ".tgz", ".tar.gz" };

  private static final String START_SCRIPT_LOCATION =
      Client.class.getClassLoader().getResource(DynoConstants.START_SCRIPT.getResourcePath()).toString();

  private YarnClient yarnClient;
  // Application master specific info to register a new Application with RM/ASM
  private String appName = "";
  // Queue for App master
  private String amQueue = "";
  // Amt. of memory resource to request for to run the App Master
  private int amMemory = 10;
  // Amt. of virtual core resource to request for to run the App Master
  private int amVCores = 1;

  // Dependency JARs. Should include, at minimum, the JAR for the App Master
  private final String[] dependencyJars;

  private String hadoopBinary = "";
  // Location of DN conf zip
  private String confPath = "";
  // Location of root dir for DN block image zips
  private String blockListPath = "";
  // Location of NN fs image
  private String fsImagePath = "";
  // Location of NN fs image md5 file
  private String fsImageMD5Path = "";
  // Location of NN VERSION file
  private String versionFilePath = "";
  // Service RPC address of the NameNode, if it is external
  private String remoteNameNodeRpcAddress = "";
  // True iff the NameNode should be launched within YARN
  private boolean launchNameNode;
  // The path to the file which contains the delegation tokens to be used for the launched
  // containers (may be null)
  private String tokenFileLocation;

  // Holds all of the options which are passed to the AM
  private AMOptions amOptions;

  // The ApplicationId of the YARN infrastructure application.
  private ApplicationId infraAppId;
  // The current state of the YARN infrastructure application.
  private volatile YarnApplicationState infraAppState = YarnApplicationState.NEW;
  private volatile JobStatus.State workloadAppState = JobStatus.State.PREP;
  // Total number of DataNodes which will be launched.
  private int numTotalDataNodes;

  // Whether or not the workload job should be launched.
  private boolean launchWorkloadJob = false;
  // The workload job itself.
  private volatile Job workloadJob;
  // The input path for the workload job.
  private String workloadInputPath = "";
  // The number of threads to use per mapper for the workload job.
  private int workloadThreadsPerMapper;
  // The startup delay for the workload job.
  private long workloadStartDelayMs;
  private double workloadRateFactor = 0.0;
  private Map<String, String> workloadExtraConfigs;

  // Start time for client
  private final long clientStartTime = System.currentTimeMillis();
  // Timeout threshold for client. Kill app after time interval expires.
  private long clientTimeout;

  // Command line options
  private Options opts;
  /**
   * @param args Command line arguments
   */
  public static void main(String[] args) throws Exception {
    Client client = new Client(ClassUtil.findContainingJar(ApplicationMaster.class));
    System.exit(ToolRunner.run(new YarnConfiguration(), client, args));
  }

  public int run(String[] args) {
    boolean result;
    try {
      LOG.info("Initializing Client");
      try {
        boolean doRun = init(args);
        if (!doRun) {
          return 0;
        }
      } catch (IllegalArgumentException e) {
        System.err.println(e.getLocalizedMessage());
        printUsage();
        return -1;
      }
      result = run();
    } catch (Throwable t) {
      LOG.fatal("Error running Client", t);
      return 1;
    }
    if (result) {
      LOG.info("Application completed successfully");
      return 0;
    }
    LOG.error("Application failed to complete successfully");
    return 2;
  }

  public Client(String... dependencyJars) {
    Preconditions.checkArgument(dependencyJars != null && dependencyJars.length > 0,
        "Must specify at least one dependency JAR for the ApplicationMaster");
    this.dependencyJars = dependencyJars;
    opts = new Options();
    opts.addOption(APPNAME_ARG, true, "Application Name. (default '" + APPNAME_DEFAULT + "')");
    opts.addOption(QUEUE_ARG, true, "RM Queue in which this application is to be submitted (default '" +
        QUEUE_DEFAULT + "')");
    opts.addOption(TIMEOUT_ARG, true, "Application timeout in milliseconds (default " +
        TIMEOUT_DEFAULT + " = unlimited)");
    opts.addOption(MASTER_MEMORY_MB_ARG, true, "Amount of memory in MB to be requested to run the " +
        "application master (default " + MASTER_MEMORY_MB_DEFAULT + ")");
    opts.addOption(MASTER_VCORES_ARG, true, "Amount of virtual cores to be requested to run the " +
        "application master (default " + MASTER_VCORES_DEFAULT + ")");
    // Dynamometer
    opts.addOption(CONF_PATH_ARG, true, "Location of the directory or archive containing the Hadoop configuration. "
        + "If this is already on a remote FS, will save the copy step, but must be an archive file. "
        + "This must have the standard Hadoop conf layout containing e.g. etc/hadoop/*-site.xml");
    opts.addOption(BLOCK_LIST_PATH_ARG, true, "Location on HDFS of the files containing the DN block lists.");
    opts.addOption(FS_IMAGE_DIR_ARG, true, "Location of the directory containing, at minimum, the VERSION file " +
        "for the namenode. If running the namenode within YARN (namenode_info_path is not specified), this " +
        "must also include the fsimage file and its md5 hash with names conforming to: `fsimage_XXXXXXXX[.md5]`.");
    for (String option : new String[] {CONF_PATH_ARG, BLOCK_LIST_PATH_ARG, FS_IMAGE_DIR_ARG}) {
      opts.getOption(option).setRequired(true);
    }
    OptionGroup hadoopBinaryGroup = new OptionGroup();
    hadoopBinaryGroup.addOption(new Option(HADOOP_BINARY_PATH_ARG, true,
        "Location of Hadoop binary to be deployed (archive). One of this or hadoop_version is required."));
    hadoopBinaryGroup.addOption(new Option(HADOOP_VERSION_ARG, true, "Version of Hadoop (like '2.7.4' or " +
        "'3.0.0-beta1') for which to download a binary. If this is specified, a Hadoop tarball will be downloaded " +
        "from an Apache mirror. By default the Berkeley OCF mirror is used; specify " +
        DynoInfraUtils.APACHE_DOWNLOAD_MIRROR_KEY + " as a configuration or system property to change which mirror " +
        "is used. The tarball will be downloaded to the working directory. One of this or hadoop_binary_path " +
        "is required."));
    hadoopBinaryGroup.setRequired(true);
    opts.addOptionGroup(hadoopBinaryGroup);
    opts.addOption(NAMENODE_SERVICERPC_ADDR_ARG, true, "Specify this option to run the NameNode " +
        "external to YARN. This is the service RPC address of the NameNode, e.g. localhost:9020.");
    opts.addOption(TOKEN_FILE_LOCATION_ARG, true, "If specified, this file will be used as the delegation token(s) " +
        "for the launched containers. Otherwise, the delegation token(s) for the default FileSystem will be used.");
    AMOptions.setOptions(opts);

    opts.addOption(WORKLOAD_REPLAY_ENABLE_ARG, false, "If specified, this client will additionally launch the workload "
        + "replay job to replay audit logs against the HDFS cluster which is started.");
    opts.addOption(WORKLOAD_INPUT_PATH_ARG, true, "Location of the audit traces to replay (Required for workload)");
    opts.addOption(WORKLOAD_THREADS_PER_MAPPER_ARG, true, "Number of threads per mapper to use to " +
        "replay the workload. (default " + AuditReplayMapper.NUM_THREADS_DEFAULT + ")");
    opts.addOption(WORKLOAD_START_DELAY_ARG, true, "Delay between launching the Workload MR job and " +
        "starting the audit logic replay; this is used in an attempt to allow all mappers to be launched before " +
        "any of them start replaying. Workloads with more mappers may need a longer delay to get all of " +
        "the containers allocated. Human-readable units accepted (e.g. 30s, 10m). " +
        "(default " + WorkloadDriver.START_TIME_OFFSET_DEFAULT + ")");
    opts.addOption(WORKLOAD_RATE_FACTOR_ARG, true,
        "Rate factor (multiplicative speed factor) to apply to workload replay (Default " +
            WORKLOAD_RATE_FACTOR_DEFAULT + ")");
    opts.addOption(WORKLOAD_CONFIG_ARG, true, "Additional configurations to pass only to the workload job. " +
        "This can be used multiple times and should be specified as a key=value pair, e.g. '-" +
        WORKLOAD_CONFIG_ARG + " conf.one=val1 -" + WORKLOAD_CONFIG_ARG + " conf.two=val2'");
  }

  /**
   * Helper function to print out usage
   */
  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setWidth(100); // Option names are long so increasing the width is helpful
    formatter.printHelp("Client", opts);
  }

  /**
   * Parse command line options
   * @param args Parsed command line options
   * @return Whether the init was successful to run the client
   * @throws ParseException
   */
  public boolean init(String[] args) throws ParseException, IOException {

    CommandLineParser parser = new GnuParser();
    if (parser.parse(new Options().addOption("h", "help", false, "Shows this message."), args, true).hasOption("h")) {
      printUsage();
      return false;
    }

    CommandLine cliParser = parser.parse(opts, args);

    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(getConf());

    LOG.info("Starting with arguments: [\"" + Joiner.on("\" \"").join(args) + "\"]");

    Path fsImageDir = new Path(cliParser.getOptionValue(FS_IMAGE_DIR_ARG, ""));
    versionFilePath = new Path(fsImageDir, "VERSION").toString();
    if (cliParser.hasOption(NAMENODE_SERVICERPC_ADDR_ARG)) {
      launchNameNode = false;
      remoteNameNodeRpcAddress = cliParser.getOptionValue(NAMENODE_SERVICERPC_ADDR_ARG);
    } else {
      launchNameNode = true;
      FileSystem localFS = FileSystem.getLocal(getConf());
      fsImageDir = fsImageDir.makeQualified(localFS.getUri(), localFS.getWorkingDirectory());
      FileSystem fsImageFS = fsImageDir.getFileSystem(getConf());
      FileStatus[] fsImageFiles = fsImageFS.listStatus(fsImageDir, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          String name = path.getName();
          return name.matches("^fsimage_(\\d)+$");
        }
      });
      if (fsImageFiles.length != 1) {
        throw new IllegalArgumentException("Must be exactly one fsimage file present in fs_image_dir");
      }
      fsImagePath = fsImageFiles[0].getPath().toString();
      fsImageMD5Path = fsImageFiles[0].getPath().suffix(".md5").toString();
    }

    if (amMemory < 0) {
      throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
          + " Specified memory=" + amMemory);
    }
    if (amVCores < 0) {
      throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
          + " Specified virtual cores=" + amVCores);
    }

    this.appName = cliParser.getOptionValue(APPNAME_ARG, APPNAME_DEFAULT);
    this.amQueue = cliParser.getOptionValue(QUEUE_ARG, QUEUE_DEFAULT);
    this.amMemory = Integer.parseInt(cliParser.getOptionValue(MASTER_MEMORY_MB_ARG, MASTER_MEMORY_MB_DEFAULT));
    this.amVCores = Integer.parseInt(cliParser.getOptionValue(MASTER_VCORES_ARG, MASTER_VCORES_DEFAULT));
    this.confPath = cliParser.getOptionValue(CONF_PATH_ARG);
    this.blockListPath = cliParser.getOptionValue(BLOCK_LIST_PATH_ARG);
    if (cliParser.hasOption(HADOOP_BINARY_PATH_ARG)) {
      this.hadoopBinary = cliParser.getOptionValue(HADOOP_BINARY_PATH_ARG);
    } else {
      this.hadoopBinary = DynoInfraUtils.fetchHadoopTarball(new File(".").getAbsoluteFile(),
          cliParser.getOptionValue(HADOOP_VERSION_ARG), getConf(), LOG).toString();
    }
    this.amOptions = AMOptions.initFromParser(cliParser);
    this.clientTimeout = Integer.parseInt(cliParser.getOptionValue(TIMEOUT_ARG, TIMEOUT_DEFAULT));
    this.tokenFileLocation = cliParser.getOptionValue(TOKEN_FILE_LOCATION_ARG);

    amOptions.verify();

    Path blockPath = new Path(blockListPath);
    FileSystem blockListFS = blockPath.getFileSystem(getConf());
    if (blockListFS.getUri().equals(FileSystem.getLocal(getConf()).getUri()) || !blockListFS.exists(blockPath)) {
      throw new IllegalArgumentException("block list path must already exist on remote fs!");
    }
    numTotalDataNodes = blockListFS.listStatus(blockPath, DynoConstants.BLOCK_LIST_FILE_FILTER).length;

    if (cliParser.hasOption(WORKLOAD_REPLAY_ENABLE_ARG)) {
      if (!cliParser.hasOption(WORKLOAD_INPUT_PATH_ARG) || !cliParser.hasOption(WORKLOAD_START_DELAY_ARG)) {
        throw new IllegalArgumentException("workload_replay_enable was specified; must include all " +
            "required workload_ parameters.");
      }
      launchWorkloadJob = true;
      workloadInputPath = cliParser.getOptionValue(WORKLOAD_INPUT_PATH_ARG);
      workloadThreadsPerMapper = Integer.parseInt(cliParser.getOptionValue(WORKLOAD_THREADS_PER_MAPPER_ARG,
          String.valueOf(AuditReplayMapper.NUM_THREADS_DEFAULT)));
      workloadRateFactor = Double.parseDouble(cliParser.getOptionValue(WORKLOAD_RATE_FACTOR_ARG,
          WORKLOAD_RATE_FACTOR_DEFAULT));
      workloadExtraConfigs = new HashMap<>();
      if (cliParser.getOptionValues(WORKLOAD_CONFIG_ARG) != null) {
        for (String opt : cliParser.getOptionValues(WORKLOAD_CONFIG_ARG)) {
          List<String> kvPair = Splitter.on("=").trimResults().splitToList(opt);
          workloadExtraConfigs.put(kvPair.get(0), kvPair.get(1));
        }
      }
      String delayString = cliParser.getOptionValue(WORKLOAD_START_DELAY_ARG, WorkloadDriver.START_TIME_OFFSET_DEFAULT);
      // Store a temporary config to leverage Configuration's time duration parsing.
      getConf().set("___temp___", delayString);
      workloadStartDelayMs = getConf().getTimeDuration("___temp___", 0, TimeUnit.MILLISECONDS);
    }

    return true;
  }

  /**
   * Main run function for the client
   * @return true if application completed successfully
   * @throws IOException
   * @throws YarnException
   */
  public boolean run() throws IOException, YarnException {

    LOG.info("Running Client");
    yarnClient.start();

    YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
    LOG.info("Got Cluster metric info from ASM, numNodeManagers=" + clusterMetrics.getNumNodeManagers());

    QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);
    LOG.info("Queue info"
        + ", queueName=" + queueInfo.getQueueName()
        + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
        + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
        + ", queueApplicationCount=" + queueInfo.getApplications().size()
        + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

    // Get a new application id
    YarnClientApplication app = yarnClient.createApplication();
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
    int maxMem = appResponse.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
    int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
    LOG.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);
    if (amMemory > maxMem || amMemory < 0 || amVCores > maxVCores || amVCores < 0) {
      throw new IllegalArgumentException("Invalid AM memory or vcores: memory=" + amMemory + ", vcores=" + amVCores);
    }
    amOptions.verify(maxMem, maxVCores);

    // set the application name
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    infraAppId = appContext.getApplicationId();
    appContext.setApplicationName(appName);

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
    Map<ApplicationAccessType, String> acls = new HashMap<>();
    acls.put(ApplicationAccessType.VIEW_APP, getConf().get(
        MRJobConfig.JOB_ACL_VIEW_JOB, MRJobConfig.DEFAULT_JOB_ACL_VIEW_JOB));
    amContainer.setApplicationACLs(acls);

    FileSystem fs = FileSystem.get(getConf());
    fs.mkdirs(getRemoteStoragePath(getConf(), infraAppId));

    // Set the env variables to be setup in the env where the application master will be run
    Map<String, String> env = setupRemoteResourcesGetEnv();

    amContainer.setEnvironment(env);

    // All of the resources for both AM and NN/DNs have been put on remote storage
    // Only the application master JAR is needed as a local resource for the AM so
    // we explicitly add it here
    Map<String, LocalResource> localResources = new HashMap<>();
    LocalResource scRsrc = LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromPath(DynoConstants.DYNO_DEPENDENCIES.getPath(env)),
        LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION,
        DynoConstants.DYNO_DEPENDENCIES.getLength(env), DynoConstants.DYNO_DEPENDENCIES.getTimestamp(env));
    localResources.put(DynoConstants.DYNO_DEPENDENCIES.getResourcePath(), scRsrc);
    // Set local resource info into app master container launch context
    amContainer.setLocalResources(localResources);

    // Set the necessary command to execute the application master
    amContainer.setCommands(getAMCommand());

    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(amMemory);
    capability.setVirtualCores(amVCores);
    appContext.setResource(capability);

    // Setup security tokens
    if (UserGroupInformation.isSecurityEnabled()) {
      ByteBuffer fsTokens;
      if (tokenFileLocation != null) {
        fsTokens = ByteBuffer.wrap(Files.readAllBytes(Paths.get(tokenFileLocation)));
      } else {
        Credentials credentials = new Credentials();
        String tokenRenewer = getConf().get(YarnConfiguration.RM_PRINCIPAL);
        if (tokenRenewer == null || tokenRenewer.length() == 0) {
          throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer");
        }

        // For now, only getting tokens for the default file-system.
        final Token<?>[] tokens = fs.addDelegationTokens(tokenRenewer, credentials);
        if (tokens != null) {
          for (Token<?> token : tokens) {
            LOG.info("Got dt for " + fs.getUri() + "; " + token);
          }
        }
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      }
      amContainer.setTokens(fsTokens);
    }

    appContext.setAMContainerSpec(amContainer);

    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue(amQueue);

    LOG.info("Submitting application to RM");
    yarnClient.submitApplication(appContext);

    // Monitor the application
    return monitorInfraApplication();

  }

  /**
   * Set up the remote resources for the application. Upload them to remote
   * storage as necessary, and set up the requisite environment variables.
   * Does not set up any local resources.
   * @return A Map representing the environment to be used for the
   *         ApplicationMaster containing the information about all of the
   *         remote resources.
   */
  private Map<String, String> setupRemoteResourcesGetEnv() throws IOException {
    LOG.info("Set the environment for the application master");
    Map<String, String> env = new HashMap<>();

    // Copy local resources to a remote FS to prepare them for localization
    // by containers. We do not need to set them as local resources here as
    // the AM does not need them.
    if (launchNameNode) {
      setupRemoteResource(infraAppId, DynoConstants.FS_IMAGE, env, fsImagePath);
      setupRemoteResource(infraAppId, DynoConstants.FS_IMAGE_MD5, env, fsImageMD5Path);
    } else {
      env.put(DynoConstants.REMOTE_NN_RPC_ADDR_ENV, remoteNameNodeRpcAddress);
    }
    setupRemoteResource(infraAppId, DynoConstants.VERSION, env, versionFilePath);
    setupRemoteResource(infraAppId, DynoConstants.CONF_ZIP, env, confPath);
    setupRemoteResource(infraAppId, DynoConstants.START_SCRIPT, env, START_SCRIPT_LOCATION);
    setupRemoteResource(infraAppId, DynoConstants.HADOOP_BINARY, env, hadoopBinary);
    setupRemoteResource(infraAppId, DynoConstants.DYNO_DEPENDENCIES, env, dependencyJars);

    env.put(DynoConstants.BLOCK_LIST_PATH_ENV, blockListPath);
    env.put(DynoConstants.JOB_ACL_VIEW_ENV,
        getConf().get(MRJobConfig.JOB_ACL_VIEW_JOB, MRJobConfig.DEFAULT_JOB_ACL_VIEW_JOB));

    env.put(DynoConstants.REMOTE_STORAGE_PATH_ENV, getRemoteStoragePath(getConf(), infraAppId).toString());

    env.put(Environment.CLASSPATH.key(), getAMClassPathEnv());
    return env;
  }

  private String getAMClassPathEnv() {
    // Add AppMaster.jar location to classpath
    // At some point we should not be required to add
    // the hadoop specific classpaths to the env.
    // It should be provided out of the box.
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$())
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
        .append("./")
        .append(DynoConstants.DYNO_DEPENDENCIES.getResourcePath())
        .append("/*");
    for (String c : getConf().getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(c.trim());
    }
    classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");

    // add the runtime classpath needed for tests to work
    if (getConf().getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(StandardSystemProperty.JAVA_CLASS_PATH.value());
    }
    return classPathEnv.toString();
  }

  private List<String> getAMCommand() {
    List<String> vargs = new ArrayList<>();

    // Set java executable command
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
    // Set Xmx based on am memory size
    long appMasterHeapSize = Math.round(amMemory * 0.85);
    vargs.add("-Xmx" + appMasterHeapSize + "m");
    // Set class name
    vargs.add(ApplicationMaster.class.getCanonicalName());
    // Set params for Application Master

    amOptions.addToVargs(vargs);

    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

    LOG.info("Completed setting up app master command: " + vargs);
    return Lists.newArrayList(Joiner.on(" ").join(vargs));
  }

  /**
   * Upload a local resource to HDFS, or if it is nonlocal,
   * just set environment appropriately. The location, length and timestamp information
   * is added to AM container's environment, so it can launch containers later
   * with the correct resource settings.
   * @throws IOException
   */
  private void setupRemoteResource(ApplicationId appId, DynoResource resource, Map<String, String> env,
      String... srcPaths) throws IOException {

    FileStatus remoteFileStatus;
    Path dstPath;

    Preconditions.checkArgument(srcPaths.length > 0, "Must supply at least one source path");
    Preconditions.checkArgument(resource.getType() == LocalResourceType.ARCHIVE || srcPaths.length == 1,
        "Can only specify multiple source paths if using an ARCHIVE type");

    List<URI> srcURIs = Arrays.stream(srcPaths).map(URI::create).collect(Collectors.toList());
    Set<String> srcSchemes = srcURIs.stream().map(URI::getScheme).collect(Collectors.toSet());
    Preconditions.checkArgument(srcSchemes.size() == 1, "All source paths must have the same scheme");
    String srcScheme = srcSchemes.iterator().next();

    String srcPathString = "[" + Joiner.on(",").join(srcPaths) + "]";

    if (srcScheme == null || srcScheme.equals(FileSystem.getLocal(getConf()).getScheme()) || srcScheme.equals("jar")) {
      // Need to upload this resource to remote storage
      List<File> srcFiles = srcURIs.stream()
          .map(URI::getSchemeSpecificPart).map(File::new)
          .collect(Collectors.toList());
      Path dstPathBase = getRemoteStoragePath(getConf(), appId);
      boolean shouldArchive = srcFiles.size() > 1 || srcFiles.get(0).isDirectory()
          || (resource.getType() == LocalResourceType.ARCHIVE
          && Arrays.stream(ARCHIVE_FILE_TYPES).noneMatch(suffix -> srcFiles.get(0).getName().endsWith(suffix)));
      if (shouldArchive) {
        if ("jar".equals(srcScheme)) {
          throw new IllegalArgumentException(String.format(
              "Resources in JARs can't be auto-zipped; resource %s is ARCHIVE and src is: %s",
              resource.getResourcePath(), srcPathString));
        } else if (resource.getType() != LocalResourceType.ARCHIVE) {
          throw new IllegalArgumentException(String.format(
              "Resource type is %s but srcPaths were: %s", resource.getType(), srcPathString));
        }
        dstPath = new Path(dstPathBase, resource.getResourcePath()).suffix(".zip");
      } else {
        dstPath = new Path(dstPathBase, srcFiles.get(0).getName());
      }
      FileSystem remoteFS = dstPath.getFileSystem(getConf());
      LOG.info("Uploading resource " + resource + " from " + srcPathString + " to " + dstPath);
      try (OutputStream outputStream = remoteFS.create(dstPath, true)) {
        if ("jar".equals(srcScheme)) {
          try (InputStream inputStream = new URL(srcPaths[0]).openStream()) {
            IOUtils.copyBytes(inputStream, outputStream, getConf());
          }
        } else if (shouldArchive) {
          List<File> filesToZip;
          if (srcFiles.size() == 1 && srcFiles.get(0).isDirectory()) {
            File[] childFiles = srcFiles.get(0).listFiles();
            if (childFiles == null || childFiles.length == 0) {
              throw new IllegalArgumentException("Specified a directory to archive with no contents");
            }
            filesToZip = Lists.newArrayList(childFiles);
          } else {
            filesToZip = srcFiles;
          }
          ZipOutputStream zout = new ZipOutputStream(outputStream);
          for (File fileToZip : filesToZip) {
            addFileToZipRecursively(fileToZip.getParentFile(), fileToZip, zout);
          }
          zout.close();
        } else {
          try (InputStream inputStream = new FileInputStream(srcFiles.get(0))) {
            IOUtils.copyBytes(inputStream, outputStream, getConf());
          }
        }
      }
      remoteFileStatus = remoteFS.getFileStatus(dstPath);
    } else {
      if (srcPaths.length > 1) {
        throw new IllegalArgumentException("If resource is on remote, must be a single file: " + srcPathString);
      }
      LOG.info("Using resource " + resource + " directly from current location: " + srcPaths[0]);
      dstPath = new Path(srcPaths[0]);
      // non-local file system; we can just use it directly from where it is
      remoteFileStatus = FileSystem.get(dstPath.toUri(), getConf()).getFileStatus(dstPath);
      if (remoteFileStatus.isDirectory()) {
        throw new IllegalArgumentException("If resource is on remote filesystem, must be a file: " + srcPaths[0]);
      }
    }
    env.put(resource.getLocationEnvVar(), dstPath.toString());
    env.put(resource.getTimestampEnvVar(), String.valueOf(remoteFileStatus.getModificationTime()));
    env.put(resource.getLengthEnvVar(), String.valueOf(remoteFileStatus.getLen()));
  }

  /**
   * Get the directory on the default FS which will be used for storing files relevant
   * to this Dynamometer application. This is inside of the
   * {@value DynoConstants#DYNAMOMETER_STORAGE_DIR} directory within the submitter's
   * home directory.
   * @param conf Configuration for this application.
   * @param appId This application's ID.
   * @return Fully qualified path on the default FS.
   */
  private static Path getRemoteStoragePath(Configuration conf, ApplicationId appId)
      throws IOException {
    FileSystem remoteFS = FileSystem.get(conf);
    return remoteFS.makeQualified(new Path(remoteFS.getHomeDirectory(),
        DynoConstants.DYNAMOMETER_STORAGE_DIR + "/" + appId));
  }

  private void addFileToZipRecursively(File root, File file, ZipOutputStream out) throws IOException {

    File[] files = file.listFiles();
    if (files == null) { // Not a directory
      String relativePath = file.getAbsolutePath().substring(
          root.getAbsolutePath().length() + 1);
      try {
        FileInputStream in = new FileInputStream(file.getAbsolutePath());
        out.putNextEntry(new ZipEntry(relativePath));
        IOUtils.copyBytes(in, out, getConf(), false);
        out.closeEntry();
        in.close();
      } catch (FileNotFoundException fnfe) {
        LOG.warn("Skipping file because it is a symlink with a nonexistent target: " + file);
      }
    } else {
      for (File containedFile : files) {
        addFileToZipRecursively(root, containedFile, out);
      }
    }
  }

  /**
   * Monitor the submitted application for completion.
   * Kill application if time expires.
   * @return true if application completed successfully
   * @throws YarnException
   * @throws IOException
   */
  private boolean monitorInfraApplication() throws YarnException, IOException {

    boolean loggedApplicationInfo = false;
    boolean success = false;

    Thread namenodeMonitoringThread = new Thread() {
      @Override
      public void run() {
        Supplier<Boolean> exitCritera = new Supplier<Boolean>() {
          @Override
          public Boolean get() {
            return isCompleted(infraAppState);
          }
        };
        Optional<Properties> namenodeProperties = Optional.absent();
        while (!exitCritera.get()) {
          try {
            if (!namenodeProperties.isPresent()) {
              namenodeProperties = DynoInfraUtils.waitForAndGetNameNodeProperties(exitCritera, getConf(),
                  getNameNodeInfoPath(), LOG);
              if (namenodeProperties.isPresent()) {
                LOG.info("NameNode can be reached via HDFS at: " +
                    DynoInfraUtils.getNameNodeHdfsUri(namenodeProperties.get()));
                LOG.info("NameNode web UI available at: " +
                    DynoInfraUtils.getNameNodeWebUri(namenodeProperties.get()));
                LOG.info("NameNode can be tracked at: " +
                    DynoInfraUtils.getNameNodeTrackingUri(namenodeProperties.get()));
              } else {
                // Only happens if we should be shutting down
                break;
              }
            }
            DynoInfraUtils.waitForNameNodeStartup(namenodeProperties.get(), exitCritera, LOG);
            DynoInfraUtils.waitForNameNodeReadiness(namenodeProperties.get(), numTotalDataNodes, false,
                exitCritera, getConf(), LOG);
            break;
          } catch (IOException ioe) {
            LOG.error("Unexpected exception while waiting for NameNode readiness", ioe);
          } catch (InterruptedException ie) {
            return;
          }
        }
        if (!isCompleted(infraAppState) && launchWorkloadJob) {
          launchAndMonitorWorkloadDriver(namenodeProperties.get());
        }
      }
    };
    if (launchNameNode) {
      namenodeMonitoringThread.start();
    }

    while (true) {

      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in
      ApplicationReport report = yarnClient.getApplicationReport(infraAppId);

      if (report.getTrackingUrl() != null && !loggedApplicationInfo) {
        loggedApplicationInfo = true;
        LOG.info("Track the application at: " + report.getTrackingUrl());
        LOG.info("Kill the application using: yarn application -kill " + report.getApplicationId());
      }

      LOG.debug("Got application report from ASM for"
          + ", appId=" + infraAppId.getId()
          + ", clientToAMToken=" + report.getClientToAMToken()
          + ", appDiagnostics=" + report.getDiagnostics()
          + ", appMasterHost=" + report.getHost()
          + ", appQueue=" + report.getQueue()
          + ", appMasterRpcPort=" + report.getRpcPort()
          + ", appStartTime=" + report.getStartTime()
          + ", yarnAppState=" + report.getYarnApplicationState().toString()
          + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
          + ", appTrackingUrl=" + report.getTrackingUrl()
          + ", appUser=" + report.getUser());

      infraAppState = report.getYarnApplicationState();
      if (infraAppState == YarnApplicationState.KILLED) {
        if (!launchWorkloadJob) {
          success = true;
        } else if (workloadJob == null) {
          LOG.error("Infra app was killed before workload job was launched.");
        } else if (!workloadJob.isComplete()) {
          LOG.error("Infra app was killed before workload job completed.");
        } else if (workloadJob.isSuccessful()) {
          success = true;
        }
        LOG.info("Infra app was killed; exiting from client.");
        break;
      } else if (infraAppState == YarnApplicationState.FINISHED || infraAppState == YarnApplicationState.FAILED) {
        LOG.info("Infra app exited unexpectedly. YarnState=" + infraAppState.toString() + ". Exiting from client.");
        break;
      }

      if ((clientTimeout != -1) && (System.currentTimeMillis() > (clientStartTime + clientTimeout))) {
        LOG.info("Reached client specified timeout of " + clientTimeout + " ms for application. Killing application");
        attemptCleanup();
        break;
      }

      if (isCompleted(workloadAppState)) {
        LOG.info("Killing infrastructure app");
        try {
          forceKillApplication(infraAppId);
        } catch (YarnException | IOException e) {
          LOG.error("Exception encountered while killing infrastructure app", e);
        }
      }
    }
    if (launchNameNode) {
      try {
        namenodeMonitoringThread.interrupt();
        namenodeMonitoringThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted while joining workload job thread; continuing to cleanup.");
      }
    }
    attemptCleanup();
    return success;
  }

  /**
   * Return the path to the property file containing information about the launched NameNode.
   */
  @VisibleForTesting
  Path getNameNodeInfoPath() throws IOException {
    return new Path(getRemoteStoragePath(getConf(), infraAppId), DynoConstants.NN_INFO_FILE_NAME);
  }

  /**
   * Launch the workload driver ({@link WorkloadDriver}) and monitor the job. Waits for the launched
   * job to complete.
   * @param nameNodeProperties The set of properties with information about the launched NameNode.
   */
  private void launchAndMonitorWorkloadDriver(Properties nameNodeProperties) {
    URI nameNodeURI = DynoInfraUtils.getNameNodeHdfsUri(nameNodeProperties);
    LOG.info("Launching workload job using input path: " + workloadInputPath);
    try {
      long workloadStartTime = System.currentTimeMillis() + workloadStartDelayMs;
      Configuration workloadConf = new Configuration(getConf());
      workloadConf.set(AuditReplayMapper.INPUT_PATH_KEY, workloadInputPath);
      workloadConf.setInt(AuditReplayMapper.NUM_THREADS_KEY, workloadThreadsPerMapper);
      workloadConf.setDouble(AuditReplayMapper.RATE_FACTOR_KEY, workloadRateFactor);
      for (Map.Entry<String, String> configPair : workloadExtraConfigs.entrySet()) {
        workloadConf.set(configPair.getKey(), configPair.getValue());
      }
      workloadJob = WorkloadDriver.getJobForSubmission(workloadConf, nameNodeURI.toString(),
          workloadStartTime, AuditReplayMapper.class);
      workloadJob.submit();
      while (!isCompleted(infraAppState) && !isCompleted(workloadAppState)) {
        workloadJob.monitorAndPrintJob();
        Thread.sleep(5000);
        workloadAppState = workloadJob.getJobState();
      }
      if (isCompleted(workloadAppState)) {
        LOG.info("Workload job completed successfully!");
      } else {
        LOG.warn("Workload job failed.");
      }
    } catch (Exception e) {
      LOG.error("Exception encountered while running workload job", e);
    }
  }

  /**
   * Best-effort attempt to clean up any remaining applications (infrastructure or workload).
   */
  public void attemptCleanup() {
    LOG.info("Attempting to clean up remaining running applications.");
    if (workloadJob != null) {
      try {
        workloadAppState = workloadJob.getJobState();
      } catch (IOException ioe) {
        LOG.warn("Unable to fetch completion status of workload job. Will proceed to attempt to kill it.", ioe);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return;
      }
      if (!isCompleted(workloadAppState)) {
        try {
          LOG.info("Attempting to kill workload app: " + workloadJob.getJobID());
          workloadJob.killJob();
          LOG.info("Killed workload app");
        } catch (IOException ioe) {
          LOG.error("Unable to kill workload app (" + workloadJob.getJobID() + ") due to ", ioe);
        }
      }
    }
    if (infraAppId != null && !isCompleted(infraAppState)) {
      try {
        LOG.info("Attempting to kill infrastructure app: " + infraAppId);
        forceKillApplication(infraAppId);
        LOG.info("Killed infrastructure app");
      } catch (YarnException|IOException e) {
        LOG.error("Unable to kill infrastructure app (" + infraAppId + ") due to ", e);
      }
    }
  }

  /**
   * Check if the input state represents completion.
   */
  private static boolean isCompleted(JobStatus.State state) {
    return state == JobStatus.State.SUCCEEDED || state == JobStatus.State.FAILED ||
        state == JobStatus.State.KILLED;
  }

  /**
   * Check if the input state represents completion.
   */
  private static boolean isCompleted(YarnApplicationState state) {
    return state == YarnApplicationState.FINISHED || state == YarnApplicationState.FAILED
        || state == YarnApplicationState.KILLED;
  }

  /**
   * Kill a submitted application by sending a call to the ASM
   * @param appId Application Id to be killed.
   * @throws YarnException
   * @throws IOException
   */
  private void forceKillApplication(ApplicationId appId)
      throws YarnException, IOException {
    // Response can be ignored as it is non-null on success or
    // throws an exception in case of failures
    yarnClient.killApplication(appId);
  }

  @VisibleForTesting
  Job getWorkloadJob() {
    return workloadJob;
  }
}
