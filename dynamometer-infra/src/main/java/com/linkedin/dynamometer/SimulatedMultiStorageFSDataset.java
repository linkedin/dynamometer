/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.dynamometer;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.ChunkChecksum;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaAlreadyExistsException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipelineInterface;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.UnexpectedReplicaStateException;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.DataChecksum;


/**
 * <p>
 * This is a modified version of {@link SimulatedFSDataset} from Hadoop branch-2, because previous versions
 * did not support having multiple storages per DataNode, which is very important for obtaining correct
 * full block report performance (as there is one report per storage). It is essentially the branch-2
 * {@link SimulatedFSDataset} with HDFS-12818 additionally applied on top of it.
 *
 * This file is a modified version of the {@link SimulatedFSDataset} class, taken from
 * <a href="http://hadoop.apache.org/">Apache Hadoop 2.7.4</a>. It was originally developed by
 * <a href="http://www.apache.org/">The Apache Software Foundation</a>.
 */
public class SimulatedMultiStorageFSDataset extends SimulatedFSDataset {
  static class Factory extends FsDatasetSpi.Factory<SimulatedMultiStorageFSDataset> {
    @Override
    public SimulatedMultiStorageFSDataset newInstance(DataNode datanode,
        DataStorage storage, Configuration conf) throws IOException {
      return new SimulatedMultiStorageFSDataset(datanode, conf);
    }

    @Override
    public boolean isSimulated() {
      return true;
    }
  }

  public static void setFactory(Configuration conf) {
    conf.set(DFSConfigKeys.DFS_DATANODE_FSDATASET_FACTORY_KEY,
        Factory.class.getName());
  }

  public static final String CONFIG_PROPERTY_CAPACITY =
      "dfs.datanode.simulateddatastorage.capacity";

  public static final long DEFAULT_CAPACITY = 2L<<40; // 1 terabyte
  public static final byte DEFAULT_DATABYTE = 9;

  public static final String CONFIG_PROPERTY_STATE =
      "dfs.datanode.simulateddatastorage.state";
  private static final DatanodeStorage.State DEFAULT_STATE =
      DatanodeStorage.State.NORMAL;

  static final byte[] nullCrcFileData;
  static {
    DataChecksum checksum = DataChecksum.newDataChecksum(
        DataChecksum.Type.NULL, 16*1024 );
    byte[] nullCrcHeader = checksum.getHeader();
    nullCrcFileData =  new byte[2 + nullCrcHeader.length];
    nullCrcFileData[0] = (byte) ((BlockMetadataHeader.VERSION >>> 8) & 0xff);
    nullCrcFileData[1] = (byte) (BlockMetadataHeader.VERSION & 0xff);
    for (int i = 0; i < nullCrcHeader.length; i++) {
      nullCrcFileData[i+2] = nullCrcHeader[i];
    }
  }

  // information about a single block
  private class BInfo implements ReplicaInPipelineInterface {
    final Block theBlock;
    private boolean finalized = false; // if not finalized => ongoing creation
    SimulatedOutputStream oStream = null;
    private long bytesAcked;
    private long bytesRcvd;
    private boolean pinned = false;
    BInfo(String bpid, Block b, boolean forWriting) throws IOException {
      theBlock = new Block(b);
      if (theBlock.getNumBytes() < 0) {
        theBlock.setNumBytes(0);
      }
      if (!getStorage(theBlock).alloc(bpid, theBlock.getNumBytes())) {
        // expected length - actual length may
        // be more - we find out at finalize
        DataNode.LOG.warn("Lack of free storage on a block alloc");
        throw new IOException("Creating block, no free space available");
      }

      if (forWriting) {
        finalized = false;
        oStream = new SimulatedOutputStream();
      } else {
        finalized = true;
        oStream = null;
      }
    }

    @Override
    public String getStorageUuid() {
      return getStorage(theBlock).getStorageUuid();
    }

    @Override
    synchronized public long getGenerationStamp() {
      return theBlock.getGenerationStamp();
    }

    @Override
    synchronized public long getNumBytes() {
      if (!finalized) {
        return bytesRcvd;
      } else {
        return theBlock.getNumBytes();
      }
    }

    @Override
    synchronized public void setNumBytes(long length) {
      if (!finalized) {
        bytesRcvd = length;
      } else {
        theBlock.setNumBytes(length);
      }
    }

    synchronized SimulatedInputStream getIStream() {
      if (!finalized) {
        // throw new IOException("Trying to read an unfinalized block");
        return new SimulatedInputStream(oStream.getLength(), DEFAULT_DATABYTE);
      } else {
        return new SimulatedInputStream(theBlock.getNumBytes(), DEFAULT_DATABYTE);
      }
    }

    synchronized void finalizeBlock(String bpid, long finalSize)
        throws IOException {
      if (finalized) {
        throw new IOException(
            "Finalizing a block that has already been finalized" +
                theBlock.getBlockId());
      }
      if (oStream == null) {
        DataNode.LOG.error("Null oStream on unfinalized block - bug");
        throw new IOException("Unexpected error on finalize");
      }

      if (oStream.getLength() != finalSize) {
        DataNode.LOG.warn("Size passed to finalize (" + finalSize +
            ")does not match what was written:" + oStream.getLength());
        throw new IOException(
            "Size passed to finalize does not match the amount of data written");
      }
      // We had allocated the expected length when block was created; 
      // adjust if necessary
      long extraLen = finalSize - theBlock.getNumBytes();
      if (extraLen > 0) {
        if (!getStorage(theBlock).alloc(bpid,extraLen)) {
          DataNode.LOG.warn("Lack of free storage on a block alloc");
          throw new IOException("Creating block, no free space available");
        }
      } else {
        getStorage(theBlock).free(bpid, -extraLen);
      }
      theBlock.setNumBytes(finalSize);

      finalized = true;
      oStream = null;
      return;
    }

    synchronized void unfinalizeBlock() throws IOException {
      if (!finalized) {
        throw new IOException("Unfinalized a block that's not finalized "
            + theBlock);
      }
      finalized = false;
      oStream = new SimulatedOutputStream();
      long blockLen = theBlock.getNumBytes();
      oStream.setLength(blockLen);
      bytesRcvd = blockLen;
      bytesAcked = blockLen;
    }

    SimulatedInputStream getMetaIStream() {
      return new SimulatedInputStream(nullCrcFileData);
    }

    synchronized boolean isFinalized() {
      return finalized;
    }

    @Override
    synchronized public ReplicaOutputStreams createStreams(boolean isCreate,
        DataChecksum requestedChecksum) throws IOException {
      if (finalized) {
        throw new IOException("Trying to write to a finalized replica "
            + theBlock);
      } else {
        SimulatedOutputStream crcStream = new SimulatedOutputStream();
        return new ReplicaOutputStreams(oStream, crcStream, requestedChecksum,
            getStorage(theBlock).getVolume().isTransientStorage());
      }
    }

    @Override
    synchronized public long getBlockId() {
      return theBlock.getBlockId();
    }

    @Override
    synchronized public long getVisibleLength() {
      return getBytesAcked();
    }

    @Override
    public ReplicaState getState() {
      return finalized ? ReplicaState.FINALIZED : ReplicaState.RBW;
    }

    @Override
    synchronized public long getBytesAcked() {
      if (finalized) {
        return theBlock.getNumBytes();
      } else {
        return bytesAcked;
      }
    }

    @Override
    synchronized public void setBytesAcked(long bytesAcked) {
      if (!finalized) {
        this.bytesAcked = bytesAcked;
      }
    }

    @Override
    public void releaseAllBytesReserved() {
    }

    @Override
    synchronized public long getBytesOnDisk() {
      if (finalized) {
        return theBlock.getNumBytes();
      } else {
        return oStream.getLength();
      }
    }

    @Override
    public void setLastChecksumAndDataLen(long dataLength, byte[] lastChecksum) {
      oStream.setLength(dataLength);
    }

    @Override
    public ChunkChecksum getLastChecksumAndDataLen() {
      return new ChunkChecksum(oStream.getLength(), null);
    }

    @Override
    public boolean isOnTransientStorage() {
      return false;
    }

    public OutputStream createRestartMetaStream() throws IOException {
      return null;
    }
  }

  /**
   * Class is used for tracking block pool storage
   */
  private static class SimulatedBPStorage {
    private long used;    // in bytes
    private final Map<Block, BInfo> blockMap = new HashMap<>();

    long getUsed() {
      return used;
    }

    void alloc(long amount) {
      used += amount;
    }

    void free(long amount) {
      used -= amount;
    }

    Map<Block, BInfo> getBlockMap() {
      return blockMap;
    }

    SimulatedBPStorage() {
      used = 0;
    }
  }

  /**
   * Class used for tracking datanode level storage
   */
  private static class SimulatedStorage {
    private final Map<String, SimulatedBPStorage> map =
        new HashMap<String, SimulatedBPStorage>();

    private final long capacity;  // in bytes
    private final DatanodeStorage dnStorage;
    private final SimulatedVolume volume;

    synchronized long getFree() {
      return capacity - getUsed();
    }

    long getCapacity() {
      return capacity;
    }

    synchronized long getUsed() {
      long used = 0;
      for (SimulatedBPStorage bpStorage : map.values()) {
        used += bpStorage.getUsed();
      }
      return used;
    }

    synchronized long getBlockPoolUsed(String bpid) throws IOException {
      return getBPStorage(bpid).getUsed();
    }

    int getNumFailedVolumes() {
      return 0;
    }

    synchronized boolean alloc(String bpid, long amount) throws IOException {
      if (getFree() >= amount) {
        getBPStorage(bpid).alloc(amount);
        return true;
      }
      return false;
    }

    synchronized void free(String bpid, long amount) throws IOException {
      getBPStorage(bpid).free(amount);
    }

    SimulatedStorage(long cap, DatanodeStorage.State state) {
      capacity = cap;
      dnStorage = new DatanodeStorage(
          "SimulatedStorage-" + DatanodeStorage.generateUuid(),
          state, StorageType.DEFAULT);
      this.volume = new SimulatedVolume(this);
    }

    synchronized void addBlockPool(String bpid) {
      SimulatedBPStorage bpStorage = map.get(bpid);
      if (bpStorage != null) {
        return;
      }
      map.put(bpid, new SimulatedBPStorage());
    }

    synchronized void removeBlockPool(String bpid) {
      map.remove(bpid);
    }

    private SimulatedBPStorage getBPStorage(String bpid) throws IOException {
      SimulatedBPStorage bpStorage = map.get(bpid);
      if (bpStorage == null) {
        throw new IOException("block pool " + bpid + " not found");
      }
      return bpStorage;
    }

    String getStorageUuid() {
      return dnStorage.getStorageID();
    }

    DatanodeStorage getDnStorage() {
      return dnStorage;
    }

    synchronized StorageReport getStorageReport(String bpid) {
      return new StorageReport(dnStorage,
          false, getCapacity(), getUsed(), getFree(),
          map.get(bpid).getUsed(), 0L);
    }

    SimulatedVolume getVolume() {
      return volume;
    }

    Map<Block, BInfo> getBlockMap(String bpid) throws IOException {
      SimulatedBPStorage bpStorage = map.get(bpid);
      if (bpStorage == null) {
        throw new IOException("Nonexistent block pool: " + bpid);
      }
      return bpStorage.getBlockMap();
    }
  }

  static class SimulatedVolume implements FsVolumeSpi {
    private final SimulatedStorage storage;

    SimulatedVolume(final SimulatedStorage storage) {
      this.storage = storage;
    }

    @Override
    public FsVolumeReference obtainReference() throws ClosedChannelException {
      return null;
    }

    @Override
    public String getStorageID() {
      return storage.getStorageUuid();
    }

    @Override
    public String[] getBlockPoolList() {
      return new String[0];
    }

    @Override
    public long getAvailable() throws IOException {
      return storage.getCapacity() - storage.getUsed();
    }

    @Override
    public String getBasePath() {
      return null;
    }

    @Override
    public String getPath(String bpid) throws IOException {
      return null;
    }

    @Override
    public File getFinalizedDir(String bpid) throws IOException {
      return null;
    }

    @Override
    public StorageType getStorageType() {
      return null;
    }

    @Override
    public boolean isTransientStorage() {
      return false;
    }

    public void reserveSpaceForRbw(long bytesToReserve) {
    }

    @Override
    public void releaseReservedSpace(long bytesToRelease) {
    }

    @Override
    public BlockIterator newBlockIterator(String bpid, String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BlockIterator loadBlockIterator(String bpid, String name)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public FsDatasetSpi getDataset() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] loadLastPartialChunkChecksum(
        File blockFile, File metaFile) throws IOException {
      return null;
    }

    public void reserveSpaceForReplica(long l) {
    }

    public void releaseLockedMemory(long l) {
    }
  }

  private final List<SimulatedStorage> storages;
  private final String datanodeUuid;
  private final DataNode datanode;

  public SimulatedMultiStorageFSDataset(DataNode datanode, Configuration conf) {
    super(datanode, null, conf);
    this.datanode = datanode;
    int storageCount = DataNode.getStorageLocations(conf).size();
    this.datanodeUuid = "SimulatedDatanode-" + DataNode.generateUuid();

    this.storages = new ArrayList<>();
    for (int i = 0; i < storageCount; i++) {
      this.storages.add(new SimulatedStorage(
          conf.getLong(CONFIG_PROPERTY_CAPACITY, DEFAULT_CAPACITY),
          conf.getEnum(CONFIG_PROPERTY_STATE, DEFAULT_STATE)));
    }
  }

  public synchronized void injectBlocks(String bpid,
      Iterable<? extends Block> injectBlocks) throws IOException {
    ExtendedBlock blk = new ExtendedBlock();
    if (injectBlocks != null) {
      for (Block b: injectBlocks) { // if any blocks in list is bad, reject list
        if (b == null) {
          throw new NullPointerException("Null blocks in block list");
        }
        blk.set(bpid, b);
        if (isValidBlock(blk)) {
          DataNode.LOG.error("Block already exists in  block list; trying to add <" +
              b + "> but already have <" + getBlockMap(blk).get(blk.getLocalBlock()).theBlock + ">; skipping");
        }
      }

      List<Map<Block, BInfo>> blockMaps = new ArrayList<>();
      for (SimulatedStorage storage : storages) {
        storage.addBlockPool(bpid);
        blockMaps.add(storage.getBlockMap(bpid));
      }

      for (Block b: injectBlocks) {
        BInfo binfo = new BInfo(bpid, b, false);
        blockMaps.get((int) (b.getBlockId() % storages.size())).put(binfo.theBlock, binfo);
      }
    }
  }

  /** Get the storage that a given block lives within. */
  private SimulatedStorage getStorage(Block b) {
    return storages.get((int) (b.getBlockId() % storages.size()));
  }

  /**
   * Get the block map that a given block lives within, assuming it is within
   * block pool bpid.
   */
  private Map<Block, BInfo> getBlockMap(Block b, String bpid)
      throws IOException {
    return getStorage(b).getBlockMap(bpid);
  }

  /** Get the block map that a given block lives within. */
  private Map<Block, BInfo> getBlockMap(ExtendedBlock b) throws IOException {
    return getBlockMap(b.getLocalBlock(), b.getBlockPoolId());
  }

  @Override // FsDatasetSpi
  public synchronized void finalizeBlock(ExtendedBlock b, boolean fsyncDir)
      throws IOException {
    BInfo binfo = getBlockMap(b).get(b.getLocalBlock());
    if (binfo == null) {
      throw new IOException("Finalizing a non existing block " + b);
    }
    binfo.finalizeBlock(b.getBlockPoolId(), b.getNumBytes());
  }

  @Override // FsDatasetSpi
  public synchronized void unfinalizeBlock(ExtendedBlock b) throws IOException{
    if (isValidRbw(b)) {
      getBlockMap(b).remove(b.getLocalBlock());
    }
  }

  synchronized BlockListAsLongs getBlockReport(String bpid,
      SimulatedStorage storage) {
    BlockListAsLongs.Builder report = BlockListAsLongs.builder();
    try {
      for (BInfo b : storage.getBlockMap(bpid).values()) {
        if (b.isFinalized()) {
          report.add(b);
        }
      }
    } catch (IOException ioe) {
      // Ignore
    }
    return report.build();
  }

  @Override
  public synchronized Map<DatanodeStorage, BlockListAsLongs> getBlockReports(
      String bpid) {
    Map<DatanodeStorage, BlockListAsLongs> blockReports = new HashMap<>();
    for (SimulatedStorage storage : storages) {
      blockReports.put(storage.getDnStorage(), getBlockReport(bpid, storage));
    }
    return blockReports;
  }

  @Override // FsDatasetSpi
  public List<Long> getCacheReport(String bpid) {
    return new LinkedList<Long>();
  }

  @Override // FSDatasetMBean
  public long getCapacity() {
    long total = 0;
    for (SimulatedStorage storage : storages) {
      total += storage.getCapacity();
    }
    return total;
  }

  @Override // FSDatasetMBean
  public long getDfsUsed() {
    long total = 0;
    for (SimulatedStorage storage : storages) {
      total += storage.getUsed();
    }
    return total;
  }

  @Override // FSDatasetMBean
  public long getBlockPoolUsed(String bpid) throws IOException {
    long total = 0;
    for (SimulatedStorage storage : storages) {
      total += storage.getBlockPoolUsed(bpid);
    }
    return total;
  }

  @Override // FSDatasetMBean
  public long getRemaining() {

    long total = 0;
    for (SimulatedStorage storage : storages) {
      total += storage.getFree();
    }
    return total;
  }

  @Override // FSDatasetMBean
  public int getNumFailedVolumes() {

    int total = 0;
    for (SimulatedStorage storage : storages) {
      total += storage.getNumFailedVolumes();
    }
    return total;
  }

  @Override // FSDatasetMBean
  public String[] getFailedStorageLocations() {
    return null;
  }

  @Override // FSDatasetMBean
  public long getLastVolumeFailureDate() {
    return 0;
  }

  @Override // FSDatasetMBean
  public long getEstimatedCapacityLostTotal() {
    return 0;
  }

  @Override // FsDatasetSpi
  public VolumeFailureSummary getVolumeFailureSummary() {
    return new VolumeFailureSummary(ArrayUtils.EMPTY_STRING_ARRAY, 0, 0);
  }

  @Override // FSDatasetMBean
  public long getCacheUsed() {
    return 0l;
  }

  @Override // FSDatasetMBean
  public long getCacheCapacity() {
    return 0l;
  }

  @Override // FSDatasetMBean
  public long getNumBlocksCached() {
    return 0l;
  }

  @Override
  public long getNumBlocksFailedToCache() {
    return 0l;
  }

  @Override
  public long getNumBlocksFailedToUncache() {
    return 0l;
  }

  @Override // FsDatasetSpi
  public synchronized long getLength(ExtendedBlock b) throws IOException {
    BInfo binfo = getBlockMap(b).get(b.getLocalBlock());
    if (binfo == null) {
      throw new IOException("Finalizing a non existing block " + b);
    }
    return binfo.getNumBytes();
  }

  @Override
  @Deprecated
  public Replica getReplica(String bpid, long blockId) {
    Block b = new Block(blockId);
    try {
      return getBlockMap(b, bpid).get(b);
    } catch (IOException ioe) {
      return null;
    }
  }

  @Override
  public synchronized String getReplicaString(String bpid, long blockId) {
    Replica r = null;
    try {
      Block b = new Block(blockId);
      r = getBlockMap(b, bpid).get(b);
    } catch (IOException ioe) {
      // Ignore
    }
    return r == null? "null": r.toString();
  }

  @Override // FsDatasetSpi
  public Block getStoredBlock(String bpid, long blkid) throws IOException {
    Block b = new Block(blkid);
    try {
      BInfo binfo = getBlockMap(b, bpid).get(b);
      if (binfo == null) {
        return null;
      }
      return new Block(blkid, binfo.getGenerationStamp(), binfo.getNumBytes());
    } catch (IOException ioe) {
      return null;
    }
  }

  @Override // FsDatasetSpi
  public synchronized void invalidate(String bpid, Block[] invalidBlks)
      throws IOException {
    boolean error = false;
    if (invalidBlks == null) {
      return;
    }
    for (Block b: invalidBlks) {
      if (b == null) {
        continue;
      }
      Map<Block, BInfo> map = getBlockMap(b, bpid);
      BInfo binfo = map.get(b);
      if (binfo == null) {
        error = true;
        DataNode.LOG.warn("Invalidate: Missing block");
        continue;
      }
      getStorage(b).free(bpid, binfo.getNumBytes());
      map.remove(b);
      if (datanode != null) {
        datanode.notifyNamenodeDeletedBlock(new ExtendedBlock(bpid, b),
            binfo.getStorageUuid());
      }
    }
    if (error) {
      throw new IOException("Invalidate: Missing blocks.");
    }
  }

  @Override // FSDatasetSpi
  public void cache(String bpid, long[] cacheBlks) {
    throw new UnsupportedOperationException(
        "SimulatedMultiStorageFSDataset does not support cache operation!");
  }

  @Override // FSDatasetSpi
  public void uncache(String bpid, long[] uncacheBlks) {
    throw new UnsupportedOperationException(
        "SimulatedMultiStorageFSDataset does not support uncache operation!");
  }

  @Override // FSDatasetSpi
  public boolean isCached(String bpid, long blockId) {
    return false;
  }

  private BInfo getBInfo(final ExtendedBlock b) {
    try {
      return getBlockMap(b).get(b.getLocalBlock());
    } catch (IOException ioe) {
      return null;
    }
  }

  @Override // {@link FsDatasetSpi}
  public boolean contains(ExtendedBlock block) {
    return getBInfo(block) != null;
  }

  /**
   * Check if a block is valid.
   *
   * @param b           The block to check.
   * @param minLength   The minimum length that the block must have.  May be 0.
   * @param state       If this is null, it is ignored.  If it is non-null, we
   *                        will check that the replica has this state.
   *
   * @throws ReplicaNotFoundException          If the replica is not found
   *
   * @throws UnexpectedReplicaStateException   If the replica is not in the 
   *                                             expected state.
   */
  @Override // {@link FsDatasetSpi}
  public void checkBlock(ExtendedBlock b, long minLength, ReplicaState state)
      throws ReplicaNotFoundException, UnexpectedReplicaStateException {
    final BInfo binfo = getBInfo(b);

    if (binfo == null) {
      throw new ReplicaNotFoundException(b);
    }
    if ((state == ReplicaState.FINALIZED && !binfo.isFinalized()) ||
        (state != ReplicaState.FINALIZED && binfo.isFinalized())) {
      throw new UnexpectedReplicaStateException(b,state);
    }
  }

  @Override // FsDatasetSpi
  public synchronized boolean isValidBlock(ExtendedBlock b) {
    try {
      checkBlock(b, 0, ReplicaState.FINALIZED);
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  /* check if a block is created but not finalized */
  @Override
  public synchronized boolean isValidRbw(ExtendedBlock b) {
    try {
      checkBlock(b, 0, ReplicaState.RBW);
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return getStorageInfo();
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaHandler append(
      ExtendedBlock b, long newGS, long expectedBlockLen) throws IOException {
    BInfo binfo = getBlockMap(b).get(b.getLocalBlock());
    if (binfo == null || !binfo.isFinalized()) {
      throw new ReplicaNotFoundException("Block " + b
          + " is not valid, and cannot be appended to.");
    }
    binfo.unfinalizeBlock();
    return new ReplicaHandler(binfo, null);
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaHandler recoverAppend(
      ExtendedBlock b, long newGS, long expectedBlockLen) throws IOException {
    final Map<Block, BInfo> map = getBlockMap(b);
    BInfo binfo = map.get(b.getLocalBlock());
    if (binfo == null) {
      throw new ReplicaNotFoundException("Block " + b
          + " is not valid, and cannot be appended to.");
    }
    if (binfo.isFinalized()) {
      binfo.unfinalizeBlock();
    }
    map.remove(b);
    binfo.theBlock.setGenerationStamp(newGS);
    map.put(binfo.theBlock, binfo);
    return new ReplicaHandler(binfo, null);
  }

  @Override // FsDatasetSpi
  public Replica recoverClose(ExtendedBlock b, long newGS, long expectedBlockLen)
      throws IOException {
    final Map<Block, BInfo> map = getBlockMap(b);
    BInfo binfo = map.get(b.getLocalBlock());
    if (binfo == null) {
      throw new ReplicaNotFoundException("Block " + b
          + " is not valid, and cannot be appended to.");
    }
    if (!binfo.isFinalized()) {
      binfo.finalizeBlock(b.getBlockPoolId(), binfo.getNumBytes());
    }
    map.remove(b.getLocalBlock());
    binfo.theBlock.setGenerationStamp(newGS);
    map.put(binfo.theBlock, binfo);
    return binfo;
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaHandler recoverRbw(
      ExtendedBlock b, long newGS, long minBytesRcvd, long maxBytesRcvd)
      throws IOException {
    final Map<Block, BInfo> map = getBlockMap(b);
    BInfo binfo = map.get(b.getLocalBlock());
    if ( binfo == null) {
      throw new ReplicaNotFoundException("Block " + b
          + " does not exist, and cannot be appended to.");
    }
    if (binfo.isFinalized()) {
      throw new ReplicaAlreadyExistsException("Block " + b
          + " is valid, and cannot be written to.");
    }
    map.remove(b);
    binfo.theBlock.setGenerationStamp(newGS);
    map.put(binfo.theBlock, binfo);
    return new ReplicaHandler(binfo, null);
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaHandler createRbw(
      StorageType storageType, ExtendedBlock b,
      boolean allowLazyPersist) throws IOException {
    return createTemporary(storageType, b, false);
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaHandler createTemporary(
      StorageType storageType, ExtendedBlock b, boolean isTransfer) throws IOException {
    if (isValidBlock(b)) {
      throw new ReplicaAlreadyExistsException("Block " + b +
          " is valid, and cannot be written to.");
    }
    if (isValidRbw(b)) {
      throw new ReplicaAlreadyExistsException("Block " + b +
          " is being written, and cannot be written to.");
    }
    BInfo binfo = new BInfo(b.getBlockPoolId(), b.getLocalBlock(), true);
    getBlockMap(b).put(binfo.theBlock, binfo);
    return new ReplicaHandler(binfo, null);
  }

  synchronized InputStream getBlockInputStream(ExtendedBlock b
  ) throws IOException {
    BInfo binfo = getBlockMap(b).get(b.getLocalBlock());
    if (binfo == null) {
      throw new IOException("No such Block " + b );
    }

    return binfo.getIStream();
  }

  @Override // FsDatasetSpi
  public synchronized InputStream getBlockInputStream(ExtendedBlock b,
      long seekOffset) throws IOException {
    InputStream result = getBlockInputStream(b);
    IOUtils.skipFully(result, seekOffset);
    return result;
  }

  /** Not supported */
  @Override // FsDatasetSpi
  public ReplicaInputStreams getTmpInputStreams(ExtendedBlock b, long blkoff,
      long ckoff) throws IOException {
    throw new IOException("Not supported");
  }

  @Override // FsDatasetSpi
  public synchronized LengthInputStream getMetaDataInputStream(ExtendedBlock b
  ) throws IOException {
    BInfo binfo = getBlockMap(b).get(b.getLocalBlock());
    if (binfo == null) {
      throw new IOException("No such Block " + b );
    }
    if (!binfo.finalized) {
      throw new IOException("Block " + b +
          " is being written, its meta cannot be read");
    }
    final SimulatedInputStream sin = binfo.getMetaIStream();
    return new LengthInputStream(sin, sin.getLength());
  }

  @Override
  public Set<File> checkDataDir() {
    // nothing to check for simulated data set
    return null;
  }

  @Override // FsDatasetSpi
  public synchronized void adjustCrcChannelPosition(ExtendedBlock b,
      ReplicaOutputStreams stream,
      int checksumSize)
      throws IOException {
  }

  /**
   * Simulated input and output streams
   *
   */
  static private class SimulatedInputStream extends java.io.InputStream {


    byte theRepeatedData = 7;
    final long length; // bytes
    int currentPos = 0;
    byte[] data = null;

    /**
     * An input stream of size l with repeated bytes
     * @param l size of the stream
     * @param iRepeatedData byte that is repeated in the stream
     */
    SimulatedInputStream(long l, byte iRepeatedData) {
      length = l;
      theRepeatedData = iRepeatedData;
    }

    /**
     * An input stream of of the supplied data
     * @param iData data to construct the stream
     */
    SimulatedInputStream(byte[] iData) {
      data = iData;
      length = data.length;
    }

    /**
     * @return the lenght of the input stream
     */
    long getLength() {
      return length;
    }

    @Override
    public int read() throws IOException {
      if (currentPos >= length)
        return -1;
      if (data !=null) {
        return data[currentPos++];
      } else {
        currentPos++;
        return theRepeatedData;
      }
    }

    @Override
    public int read(byte[] b) throws IOException {

      if (b == null) {
        throw new NullPointerException();
      }
      if (b.length == 0) {
        return 0;
      }
      if (currentPos >= length) { // EOF
        return -1;
      }
      int bytesRead = (int) Math.min(b.length, length-currentPos);
      if (data != null) {
        System.arraycopy(data, currentPos, b, 0, bytesRead);
      } else { // all data is zero
        for (int i : b) {
          b[i] = theRepeatedData;
        }
      }
      currentPos += bytesRead;
      return bytesRead;
    }
  }

  /**
   * This class implements an output stream that merely throws its data away, but records its
   * length.
   *
   */
  static private class SimulatedOutputStream extends OutputStream {
    long length = 0;

    /**
     * constructor for Simulated Output Steram
     */
    SimulatedOutputStream() {
    }

    /**
     *
     * @return the length of the data created so far.
     */
    long getLength() {
      return length;
    }

    /**
     */
    void setLength(long length) {
      this.length = length;
    }

    @Override
    public void write(int arg0) throws IOException {
      length++;
    }

    @Override
    public void write(byte[] b) throws IOException {
      length += b.length;
    }

    @Override
    public void write(byte[] b,
        int off,
        int len) throws IOException  {
      length += len;
    }
  }

  private ObjectName mbeanName;



  /**
   * Register the FSDataset MBean using the name
   *        "hadoop:service=DataNode,name=FSDatasetState-<storageid>"
   *  We use storage id for MBean name since a minicluster within a single
   * Java VM may have multiple Simulated Datanodes.
   */
  void registerMBean(final String storageId) {
    // We wrap to bypass standard mbean naming convetion.
    // This wraping can be removed in java 6 as it is more flexible in 
    // package naming for mbeans and their impl.
    StandardMBean bean;

    try {
      bean = new StandardMBean(this,FSDatasetMBean.class);
      mbeanName = MBeans.register("DataNode", "FSDatasetState-"+
          storageId, bean);
    } catch (NotCompliantMBeanException e) {
      DataNode.LOG.warn("Error registering FSDatasetState MBean", e);
    }

    DataNode.LOG.info("Registered FSDatasetState MBean");
  }

  @Override
  public void shutdown() {
    if (mbeanName != null) MBeans.unregister(mbeanName);
  }

  @Override
  public String getStorageInfo() {
    return "Simulated FSDataset-" + datanodeUuid;
  }

  @Override
  public boolean hasEnoughResource() {
    return true;
  }

  @Override
  public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
      throws IOException {
    ExtendedBlock b = rBlock.getBlock();
    BInfo binfo = getBlockMap(b).get(b.getLocalBlock());
    if (binfo == null) {
      throw new IOException("No such Block " + b );
    }

    return new ReplicaRecoveryInfo(binfo.getBlockId(), binfo.getBytesOnDisk(),
        binfo.getGenerationStamp(),
        binfo.isFinalized()?ReplicaState.FINALIZED : ReplicaState.RBW);
  }

  @Override // FsDatasetSpi
  public Replica updateReplicaUnderRecovery(ExtendedBlock oldBlock,
      long recoveryId,
      long newBlockId,
      long newlength) throws IOException {
    return getBInfo(oldBlock);
  }

  @Override // FsDatasetSpi
  public long getReplicaVisibleLength(ExtendedBlock block) {
    return block.getNumBytes();
  }

  @Override // FsDatasetSpi
  public void addBlockPool(String bpid, Configuration conf) {
    for (SimulatedStorage storage : storages) {
      storage.addBlockPool(bpid);
    }
  }

  @Override // FsDatasetSpi
  public void shutdownBlockPool(String bpid) {
    for (SimulatedStorage storage : storages) {
      storage.removeBlockPool(bpid);
    }
  }

  @Override // FsDatasetSpi
  public void deleteBlockPool(String bpid, boolean force) {
    return;
  }

  @Override
  public ReplicaInPipelineInterface convertTemporaryToRbw(ExtendedBlock temporary)
      throws IOException {
    final BInfo r = getBlockMap(temporary).get(temporary.getLocalBlock());
    if (r == null) {
      throw new IOException("Block not found, temporary=" + temporary);
    } else if (r.isFinalized()) {
      throw new IOException("Replica already finalized, temporary="
          + temporary + ", r=" + r);
    }
    return r;
  }

  @Override
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock b) {
    throw new UnsupportedOperationException();
  }

  @Override
  public HdfsBlocksMetadata getHdfsBlocksMetadata(String bpid, long[] blockIds)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void enableTrash(String bpid) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearTrash(String bpid) {
  }

  @Override
  public boolean trashEnabled(String bpid) {
    return false;
  }

  @Override
  public void setRollingUpgradeMarker(String bpid) {
  }

  @Override
  public void clearRollingUpgradeMarker(String bpid) {
  }

  @Override
  public void checkAndUpdate(String bpid, long blockId, File diskFile,
      File diskMetaFile, FsVolumeSpi vol) throws IOException {
    throw new UnsupportedOperationException();
  }

  public List<FsVolumeSpi> getVolumes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addVolume(
      final StorageLocation location,
      final List<NamespaceInfo> nsInfos) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatanodeStorage getStorage(final String storageUuid) {
    for (SimulatedStorage storage : storages) {
      if (storageUuid.equals(storage.getStorageUuid())) {
        return storage.getDnStorage();
      }
    }
    return null;
  }

  @Override
  public StorageReport[] getStorageReports(String bpid) {
    List<StorageReport> reports = new ArrayList<>();
    for (SimulatedStorage storage : storages) {
      reports.add(storage.getStorageReport(bpid));
    }
    return reports.toArray(new StorageReport[0]);
  }

  @Override
  public List<FinalizedReplica> getFinalizedBlocks(String bpid) {
    throw new UnsupportedOperationException();
  }

  public List<FinalizedReplica> getFinalizedBlocksOnPersistentStorage(String bpid) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Object> getVolumeInfoMap() {
    throw new UnsupportedOperationException();
  }

  @Override
  public FsVolumeSpi getVolume(ExtendedBlock b) {
    return getStorage(b.getLocalBlock()).getVolume();
  }

  @Override
  public synchronized void removeVolumes(Set<File> volumes, boolean clearFailure) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void submitBackgroundSyncFileRangeRequest(ExtendedBlock block,
      FileDescriptor fd, long offset, long nbytes, int flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void onCompleteLazyPersist(String bpId, long blockId,
      long creationTime, File[] savedFiles, FsVolumeSpi targetVolume) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void onFailLazyPersist(String bpId, long blockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ReplicaInfo moveBlockAcrossStorage(ExtendedBlock block,
      StorageType targetStorageType) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setPinning(ExtendedBlock b) throws IOException {
    getBlockMap(b).get(b.getLocalBlock()).pinned = true;
  }

  @Override
  public boolean getPinning(ExtendedBlock b) throws IOException {
    return getBlockMap(b).get(b.getLocalBlock()).pinned;
  }

  @Override
  public boolean isDeletingBlock(String bpid, long blockId) {
    throw new UnsupportedOperationException();
  }
}


