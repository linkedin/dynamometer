/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;


/**
 * The ApplicationMaster for Dynamometer. This will launch DataNodes
 * in YARN containers. If the RPC address of a NameNode is specified,
 * it will configure the DataNodes to talk to that NameNode. Else, a
 * NameNode will be launched as part of this YARN application.
 * This does not implement any retry/failure handling.
 * TODO: Add proper retry/failure handling
 * The AM will persist until it has run for a period of time equal to
 * the timeout specified or until the application is killed.
 *
 * If the NameNode is launched internally, it will upload some information
 * onto the remote HDFS instance (i.e., the default FileSystem) about its
 * hostname and ports. This is in the location determined by the
 * {@link DynoConstants#DYNAMOMETER_STORAGE_DIR} and
 * {@link DynoConstants#NN_INFO_FILE_NAME} constants and is in the
 * {@link Properties} file format. This is consumed by this AM as well
 * as the {@link Client} to determine how to contact the NameNode.
 *
 * Information about the location of the DataNodes is logged by the AM.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {

  // Location of the script used to start the DataNode/NameNode processes
  private static final String START_SCRIPT_PATH = "scripts/start-component.sh";

  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

  // Configuration
  private Configuration conf;

  // Handle to communicate with the Resource Manager
  @SuppressWarnings("rawtypes")
  private AMRMClientAsync amRMClient;

  // Handle to communicate with the Node Manager
  private NMClientAsync nmClientAsync;
  // Listen to process the response from the Node Manager
  private NMCallbackHandler containerListener;
  // The collection of options passed in via the Client
  private AMOptions amOptions;

  private List<LocalResource> blockListFiles;
  private int numTotalDataNodes;

  // Counter for completed datanodes (complete denotes successful or failed )
  private AtomicInteger numCompletedDataNodes = new AtomicInteger();
  // Allocated datanode count so that we know how many datanodes has the RM
  // allocated to us
  private AtomicInteger numAllocatedDataNodes = new AtomicInteger();
  // Count of failed datanodes
  private AtomicInteger numFailedDataNodes = new AtomicInteger();

  // True iff the application has completed and is ready for cleanup
  // Once true, will never be false. This variable should not be accessed
  // directly but rather through the isComplete, waitForCompletion, and
  // markCompleted methods.
  private boolean completed = false;
  private final Object completionLock = new Object();

  private ByteBuffer allTokens;

  // Launch threads
  private List<Thread> launchThreads = new ArrayList<>();

  // True iff this AM should launch and manage a Namanode
  private boolean launchNameNode;
  // The service RPC address of a remote NameNode to be contacted by the launched DataNodes
  private String namenodeServiceRpcAddress = "";
  // Directory to use for remote storage (a location on the remote FS which
  // can be accessed by all components)
  private Path remoteStoragePath;
  // The container the NameNode is running within
  private volatile Container namenodeContainer;
  // Map of the containers that the DataNodes are running within
  private ConcurrentMap<ContainerId, Container> datanodeContainers = new ConcurrentHashMap<>();

  // Username of the user who launched this application.
  private String launchingUser;

  /**
   * @param args Command line args
   */
  public static void main(String[] args) {
    boolean result = false;
    try {
      ApplicationMaster appMaster = new ApplicationMaster();
      LOG.info("Initializing ApplicationMaster");
      boolean doRun = appMaster.init(args);
      if (!doRun) {
        System.exit(0);
      }
      result = appMaster.run();
    } catch (Throwable t) {
      LOG.fatal("Error running ApplicationMaster", t);
      System.exit(1);
    }
    if (result) {
      LOG.info("Application Master completed successfully. exiting");
      System.exit(0);
    } else {
      LOG.info("Application Master failed. exiting");
      System.exit(2);
    }
  }

  public ApplicationMaster() {
    // Set up the configuration
    conf = new YarnConfiguration();
  }

  /**
   * Parse command line options
   *
   * @param args Command line args
   * @return Whether init successful and run should be invoked
   * @throws ParseException
   * @throws IOException
   */
  public boolean init(String[] args) throws ParseException, IOException {

    Options opts = new Options();
    AMOptions.setOptions(opts);
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      printUsage(opts);
      throw new IllegalArgumentException(
          "No args specified for application master to initialize");
    }

    if (cliParser.hasOption("help")) {
      printUsage(opts);
      return false;
    }

    Map<String, String> envs = System.getenv();

    remoteStoragePath = new Path(envs.get(DynoConstants.REMOTE_STORAGE_PATH_ENV));
    launchingUser = envs.get(Environment.USER.name());
    if (envs.containsKey(DynoConstants.REMOTE_NN_RPC_ADDR_ENV)) {
      launchNameNode = false;
      namenodeServiceRpcAddress = envs.get(DynoConstants.REMOTE_NN_RPC_ADDR_ENV);
    } else {
      launchNameNode = true;
      // namenodeServiceRpcAddress will be set in run() once properties are available
    }

    ContainerId containerId = ConverterUtils.toContainerId(envs.get(Environment.CONTAINER_ID.name()));
    ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();
    LOG.info("Application master for app" + ", appId="
        + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
        + appAttemptID.getApplicationId().getClusterTimestamp()
        + ", attemptId=" + appAttemptID.getAttemptId());

    amOptions = AMOptions.initFromParser(cliParser);

    return true;
  }

  /**
   * Helper function to print usage
   *
   * @param opts Parsed command line options
   */
  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("ApplicationMaster", opts);
  }

  /**
   * Main run function for the application master
   *
   * @return True if the application completed successfully; false if
   *         if exited unexpectedly, failed, was killed, etc.
   * @throws YarnException
   * @throws IOException
   */
  @SuppressWarnings({ "unchecked" })
  public boolean run() throws YarnException, IOException, InterruptedException, URISyntaxException {
    LOG.info("Starting ApplicationMaster");

    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    // Now remove the AM->RM token so that containers cannot access it.
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRMClient.init(conf);
    amRMClient.start();

    containerListener = createNMCallbackHandler();
    nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(conf);
    nmClientAsync.start();

    // Register self with ResourceManager
    // This will start heartbeating to the RM
    String appMasterHostname = NetUtils.getHostname();
    amRMClient.registerApplicationMaster(appMasterHostname, -1, "");

    // Supplier to use to indicate to wait-loops to stop waiting
    Supplier<Boolean> exitCritera = new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return isComplete();
      }
    };

    Optional<Properties> namenodeProperties = Optional.absent();
    if (launchNameNode) {
      ContainerRequest nnContainerRequest = setupContainerAskForRM(
          amOptions.getNameNodeMemoryMB(), amOptions.getNameNodeVirtualCores(), 0);
      LOG.info("Requested NameNode ask: " + nnContainerRequest.toString());
      amRMClient.addContainerRequest(nnContainerRequest);

      // Wait for the NN container to make its information available on the shared
      // remote file storage
      Path namenodeInfoPath = new Path(remoteStoragePath, DynoConstants.NN_INFO_FILE_NAME);
      LOG.info("Waiting on availability of NameNode information at " + namenodeInfoPath);

      namenodeProperties = DynoInfraUtils.waitForAndGetNameNodeProperties(exitCritera, conf, namenodeInfoPath, LOG);
      if (!namenodeProperties.isPresent()) {
        cleanup();
        return false;
      }
      namenodeServiceRpcAddress = DynoInfraUtils.getNameNodeServiceRpcAddr(namenodeProperties.get()).toString();
      LOG.info("NameNode information: " + namenodeProperties.get());
      LOG.info("NameNode can be reached at: " + DynoInfraUtils.getNameNodeHdfsUri(namenodeProperties.get()).toString());
      DynoInfraUtils.waitForNameNodeStartup(namenodeProperties.get(), exitCritera, LOG);
    } else {
      LOG.info("Using remote NameNode with RPC address: " + namenodeServiceRpcAddress);
    }

    blockListFiles = Collections.synchronizedList(getDataNodeBlockListingFiles());
    numTotalDataNodes = blockListFiles.size();
    if (numTotalDataNodes == 0) {
      LOG.error("No block listing files were found! Cannot run with 0 DataNodes.");
      markCompleted();
      return false;
    }

    LOG.info("Requesting " + numTotalDataNodes + " DataNodes with " + amOptions.getDataNodeMemoryMB() + "MB memory, " +
        amOptions.getDataNodeVirtualCores() + " vcores, ");
    for (int i = 0; i < numTotalDataNodes; ++i) {
      ContainerRequest datanodeAsk =
          setupContainerAskForRM(amOptions.getDataNodeMemoryMB(), amOptions.getDataNodeVirtualCores(), 1);
      amRMClient.addContainerRequest(datanodeAsk);
      LOG.debug("Requested datanode ask: " + datanodeAsk.toString());
    }
    LOG.info("Finished requesting datanode containers");

    if (launchNameNode) {
      DynoInfraUtils.waitForNameNodeReadiness(namenodeProperties.get(), numTotalDataNodes, exitCritera, LOG);
    }

    waitForCompletion();
    return cleanup();
  }

  NMCallbackHandler createNMCallbackHandler() {
    return new NMCallbackHandler();
  }

  /**
   * Wait until the application has finished and is ready for cleanup.
   */
  private void waitForCompletion() throws InterruptedException {
    synchronized (completionLock) {
      while (!completed) {
        completionLock.wait();
      }
    }
  }

  /**
   * Check completion status of the application.
   * @return True iff it has completed.
   */
  private boolean isComplete() {
    synchronized (completionLock) {
      return completed;
    }
  }

  /**
   * Mark that this application should begin cleaning up and exit.
   */
  private void markCompleted() {
    synchronized (completionLock) {
      completed = true;
      completionLock.notify();
    }
  }

  /**
   * @return True iff the application successfully completed
   */
  private boolean cleanup() {
    // Join all launched threads
    // needed for when we time out
    // and we need to release containers
    for (Thread launchThread : launchThreads) {
      try {
        launchThread.join(10000);
      } catch (InterruptedException e) {
        LOG.info("Exception thrown in thread join: " + e.getMessage());
        e.printStackTrace();
      }
    }

    // When the application completes, it should stop all running containers
    LOG.info("Application completed. Stopping running containers");
    nmClientAsync.stop();

    // When the application completes, it should send a finish application
    // signal to the RM
    LOG.info("Application completed. Signalling finish to RM");

    FinalApplicationStatus appStatus;
    String appMessage = null;
    boolean success;
    if (numFailedDataNodes.get() == 0 && numCompletedDataNodes.get() == numTotalDataNodes) {
      appStatus = FinalApplicationStatus.SUCCEEDED;
      success = true;
    } else {
      appStatus = FinalApplicationStatus.FAILED;
      appMessage = "Diagnostics." + ", total=" + numTotalDataNodes
          + ", completed=" + numCompletedDataNodes.get() + ", allocated="
          + numAllocatedDataNodes.get() + ", failed=" + numFailedDataNodes.get();
      success = false;
    }
    try {
      amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
    } catch (YarnException ex) {
      LOG.error("Failed to unregister application", ex);
    } catch (IOException e) {
      LOG.error("Failed to unregister application", e);
    }

    amRMClient.stop();
    return success;
  }

  private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    @SuppressWarnings("unchecked")
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      LOG.info("Got response from RM for container ask, completedCnt=" + completedContainers.size());
      for (ContainerStatus containerStatus : completedContainers) {
        String containerInfo = "containerID="
            + containerStatus.getContainerId() + ", state="
            + containerStatus.getState() + ", exitStatus="
            + containerStatus.getExitStatus() + ", diagnostics="
            + StringUtils.abbreviate(containerStatus.getDiagnostics(), 1000);
        String component;
        if (isNameNode(containerStatus.getContainerId())) {
          component = "NAMENODE";
        } else if (isDataNode(containerStatus.getContainerId())) {
          component = "DATANODE";
        } else {
          LOG.error("Received container status for unknown container: " + containerInfo);
          continue;
        }
        LOG.info("Got container status for " + component + ": " + containerInfo);

        // non complete containers should not be here
        assert (containerStatus.getState() == ContainerState.COMPLETE);

        if (component.equals("NAMENODE")) {
          LOG.info("NameNode container completed; marking application as done");
          markCompleted();
        }

        // increment counters for completed/failed containers
        int exitStatus = containerStatus.getExitStatus();
        int completed = numCompletedDataNodes.incrementAndGet();
        if (0 != exitStatus) {
          numFailedDataNodes.incrementAndGet();
        } else {
          LOG.info("DataNode " + completed + " completed successfully, containerId="
              + containerStatus.getContainerId());
        }
      }

      if (numCompletedDataNodes.get() == numTotalDataNodes) {
        LOG.info("All datanode containers completed; marking application as done");
        markCompleted();
      }
    }

    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {
      LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
      for (Container container : allocatedContainers) {
        LaunchContainerRunnable containerLauncher;
        String componentType;
        Resource rsrc = container.getResource();
        if (launchNameNode && rsrc.getMemory() >= amOptions.getNameNodeMemoryMB()
            && rsrc.getVirtualCores() >= amOptions.getNameNodeVirtualCores() && namenodeContainer == null) {
          namenodeContainer = container;
          componentType = "NAMENODE";
          containerLauncher = new LaunchContainerRunnable(container, true, containerListener);
        } else if (rsrc.getMemory() >= amOptions.getDataNodeMemoryMB()
            && rsrc.getVirtualCores() >= amOptions.getDataNodeVirtualCores()
            && numAllocatedDataNodes.get() < numTotalDataNodes) {
          if (launchNameNode && namenodeContainer == null) {
            LOG.error("Received a container with following resources suited " +
                "for a DataNode but no NameNode container exists: containerMem=" +
                rsrc.getMemory() + ", containerVcores=" + rsrc.getVirtualCores());
            continue;
          }
          numAllocatedDataNodes.getAndIncrement();
          datanodeContainers.put(container.getId(), container);
          componentType = "DATANODE";
          containerLauncher = new LaunchContainerRunnable(container, false, containerListener);
        } else {
          LOG.warn("Received unwanted container allocation: " + container);
          nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
          continue;
        }
        LOG.info("Launching " + componentType + " on a new container."
            + ", containerId=" + container.getId()
            + ", containerNode=" + container.getNodeId().getHost()
            + ":" + container.getNodeId().getPort()
            + ", containerNodeURI=" + container.getNodeHttpAddress()
            + ", containerResourceMemory=" + rsrc.getMemory()
            + ", containerResourceVirtualCores=" + rsrc.getVirtualCores());
        Thread launchThread = new Thread(containerLauncher);

        // launch and start the container on a separate thread to keep
        // the main thread unblocked
        // as all containers may not be allocated at one go.
        launchThreads.add(launchThread);
        launchThread.start();
      }
    }

    @Override
    public void onShutdownRequest() {
      markCompleted();
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {}

    @Override
    public float getProgress() {
      return 0.0f;
    }

    @Override
    public void onError(Throwable e) {
      markCompleted();
      amRMClient.stop();
    }
  }

  private class NMCallbackHandler
    implements NMClientAsync.CallbackHandler {

    @Override
    public void onContainerStopped(ContainerId containerId) {
      if (isNameNode(containerId)) {
        LOG.info("NameNode container stopped: " + containerId);
        namenodeContainer = null;
        markCompleted();
      } else if (isDataNode(containerId)) {
        LOG.debug("DataNode container stopped: " + containerId);
        datanodeContainers.remove(containerId);
      } else {
        LOG.error("onContainerStopped received unknown container ID: " + containerId);
      }
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId,
        ContainerStatus containerStatus) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Container Status: id=" + containerId + ", status=" +
            containerStatus);
      }
    }

    @Override
    public void onContainerStarted(ContainerId containerId,
        Map<String, ByteBuffer> allServiceResponse) {
      if (isNameNode(containerId)) {
        LOG.info("NameNode container started at ID " + containerId);
      } else if (isDataNode(containerId)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Succeeded to start DataNode Container " + containerId);
        }
        nmClientAsync.getContainerStatusAsync(containerId,
            datanodeContainers.get(containerId).getNodeId());
      } else {
        LOG.error("onContainerStarted received unknown container ID: " + containerId);
      }
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      if (isNameNode(containerId)) {
        LOG.error("Failed to start namenode container ID " + containerId, t);
        namenodeContainer = null;
        markCompleted();
      } else if (isDataNode(containerId)) {
        LOG.error("Failed to start DataNode Container " + containerId);
        datanodeContainers.remove(containerId);
        numCompletedDataNodes.incrementAndGet();
        numFailedDataNodes.incrementAndGet();
      } else {
        LOG.error("onStartContainerError received unknown container ID: " + containerId);
      }
    }

    @Override
    public void onGetContainerStatusError(
        ContainerId containerId, Throwable t) {
      LOG.error("Failed to query the status of Container " + containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      if (isNameNode(containerId)) {
        LOG.error("Failed to stop NameNode container ID " + containerId);
        namenodeContainer = null;
      } else if (isDataNode(containerId)) {
        LOG.error("Failed to stop DataNode Container " + containerId);
        datanodeContainers.remove(containerId);
      } else {
        LOG.error("onStopContainerError received unknown containerID: " + containerId);
      }
    }
  }

  /**
   * Thread to connect to the {@link ContainerManagementProtocol} and launch the container
   * that will execute the shell command.
   */
  private class LaunchContainerRunnable implements Runnable {

    // Allocated container
    Container container;
    boolean isNameNodeLauncher;
    NMCallbackHandler containerListener;

    /**
     * @param lcontainer Allocated container
     * @param containerListener Callback handler of the container
     */
    public LaunchContainerRunnable(
        Container lcontainer, boolean isNameNode, NMCallbackHandler containerListener) {
      this.container = lcontainer;
      this.containerListener = containerListener;
      this.isNameNodeLauncher = isNameNode;
    }

    @Override
    /**
     * Connects to CM, sets up container launch context
     * for shell command and eventually dispatches the container
     * start request to the CM.
     */
    public void run() {
      LOG.info("Setting up container launch context for containerid="
          + container.getId() + ", isNameNode=" + isNameNodeLauncher);
      ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

      // Set the environment
      ctx.setEnvironment(amOptions.getShellEnv());

      try {
        ctx.setLocalResources(getLocalResources());

        ctx.setCommands(getContainerStartCommand());
      } catch (IOException e) {
        LOG.error("Error while configuring container!", e);
        return;
      }

      // Set up tokens for the container
      ctx.setTokens(allTokens.duplicate());

      nmClientAsync.startContainerAsync(container, ctx);
      LOG.info(String.format("Starting %s; track at: http://%s/node/containerlogs/%s/%s/",
          isNameNodeLauncher ? "NAMENODE" : "DATANODE",
          container.getNodeHttpAddress(), container.getId(), launchingUser));
    }

    /**
     * Get the map of local resources to be used for launching this container.
     */
    private Map<String, LocalResource> getLocalResources() throws IOException {
      Map<String, LocalResource> localResources = new HashMap<>();

      Map<String, String> envs = System.getenv();
      addAsLocalResourceFromEnv(DynoConstants.CONF_ZIP, localResources, envs);
      addAsLocalResourceFromEnv(DynoConstants.SCRIPTS_ZIP, localResources, envs);
      addAsLocalResourceFromEnv(DynoConstants.HADOOP_BINARY, localResources, envs);
      addAsLocalResourceFromEnv(DynoConstants.VERSION, localResources, envs);
      if (isNameNodeLauncher) {
        addAsLocalResourceFromEnv(DynoConstants.FS_IMAGE, localResources, envs);
        addAsLocalResourceFromEnv(DynoConstants.FS_IMAGE_MD5, localResources, envs);
      } else {
        addAsLocalResourceFromEnv(DynoConstants.DYNO_JAR, localResources, envs);
        localResources.put(DynoConstants.BLOCK_LIST_RESOURCE_PATH, blockListFiles.remove(0));
      }
      return localResources;
    }

    /**
     * Return the command used to start this container.
     */
    private List<String> getContainerStartCommand() throws IOException {
      // Set the necessary command to execute on the allocated container
      List<String> vargs = new ArrayList<>();

      // Set executable command
      vargs.add(START_SCRIPT_PATH);
      String component = isNameNodeLauncher ? "namenode" : "datanode";
      vargs.add(component);
      if (isNameNodeLauncher) {
        vargs.add(remoteStoragePath.getFileSystem(conf).makeQualified(remoteStoragePath).toString());
      } else {
        vargs.add(namenodeServiceRpcAddress);
      }

      // Add log redirect params
      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

      LOG.info("Completed setting up command for " + component + ": " + vargs);
      return Lists.newArrayList(Joiner.on(" ").join(vargs));
    }

    /**
     * Add the given resource into the map of resources, using information
     * from the supplied environment variables.
     * @param resource The resource to add.
     * @param localResources Map of local resources to insert into.
     * @param env Map of environment variables.
     */
    public void addAsLocalResourceFromEnv(DynoResource resource,
        Map<String, LocalResource> localResources, Map<String, String> env) {
      LOG.debug("Adding resource to localResources: " + resource);
      String resourcePath = resource.getResourcePath();
      if (resourcePath == null) {
        // Default to using the file name in the path
        resourcePath = resource.getPath(env).getName();
      }
      localResources.put(resourcePath, LocalResource.newInstance(
          ConverterUtils.getYarnUrlFromPath(resource.getPath(env)),
          resource.getType(), LocalResourceVisibility.APPLICATION,
          resource.getLength(env), resource.getTimestamp(env)));
    }
  }

  private List<LocalResource> getDataNodeBlockListingFiles() throws IOException {
    Path blockListDirPath = new Path(System.getenv().get(DynoConstants.BLOCK_LIST_PATH_ENV));
    LOG.info("Looking for block listing files in " + blockListDirPath);
    FileSystem blockZipFS = blockListDirPath.getFileSystem(conf);
    List<LocalResource> files = new LinkedList<>();
    for (FileStatus stat : blockZipFS.listStatus(blockListDirPath, DynoConstants.BLOCK_LIST_FILE_FILTER)) {
      LocalResource blockListResource = LocalResource.newInstance(
          ConverterUtils.getYarnUrlFromPath(stat.getPath()),
          LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
          stat.getLen(), stat.getModificationTime());
      files.add(blockListResource);
    }
    return files;
  }

  /**
   * Return true iff {@code containerId} represents the NameNode container.
   */
  private boolean isNameNode(ContainerId containerId) {
    return namenodeContainer != null && namenodeContainer.getId().equals(containerId);
  }

  /**
   * Return true iff {@code containerId} represents a DataNode container.
   */
  private boolean isDataNode(ContainerId containerId) {
    return datanodeContainers.containsKey(containerId);
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   *
   * @return the setup ResourceRequest to be sent to RM
   */
  private ContainerRequest setupContainerAskForRM(int memory, int vcores, int priority) {
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(priority);

    // Set up resource type requirements
    // For now, memory and CPU are supported so we set memory and cpu requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(memory);
    capability.setVirtualCores(vcores);

    return new ContainerRequest(capability, null, null, pri);
  }

}

