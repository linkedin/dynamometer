/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.blockgenerator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;


/**
 * This is the MapOutputValue class. It has the blockId and the block generation stamp
 * which is needed to generate the block images in the reducer.
 *
 * This also stores the replication of the block, but note that it does not serialize
 * this value as part of its {@link Writable} interface, and does not consider
 * the replication when doing equality / hash comparisons.
 */

public class BlockInfo implements Writable {

  public LongWritable getBlockId() {
    return blockId;
  }

  public void setBlockId(LongWritable blockId) {
    this.blockId = blockId;
  }

  public LongWritable getBlockGenerationStamp() {
    return blockGenerationStamp;
  }

  public void setBlockGenerationStamp(LongWritable blockGenerationStamp) {
    this.blockGenerationStamp = blockGenerationStamp;
  }

  public LongWritable getSize() {
    return size;
  }

  public void setSize(LongWritable size) {
    this.size = size;
  }

  public short getReplication() {
    return replication;
  }

  public void setReplication(short replication) {
    this.replication = replication;
  }

  private LongWritable blockId;
  private LongWritable blockGenerationStamp;
  private LongWritable size;
  private transient short replication;

  public BlockInfo(BlockInfo blockInfo) {
    this.blockId = blockInfo.getBlockId();
    this.blockGenerationStamp = blockInfo.getBlockGenerationStamp();
    this.size = blockInfo.getSize();
    this.replication = replication;
  }

  public BlockInfo() {
    this.blockId = new LongWritable();
    this.blockGenerationStamp = new LongWritable();
    this.size = new LongWritable(1);
  }

  public BlockInfo(long blockid, long blockgenerationstamp) {
    this.blockId = new LongWritable(blockid);
    this.blockGenerationStamp = new LongWritable(blockgenerationstamp);
    this.size = new LongWritable(1);
  }

  public BlockInfo(long blockid, long blockgenerationstamp, long size) {
    this.blockId = new LongWritable(blockid);
    this.blockGenerationStamp = new LongWritable(blockgenerationstamp);
    this.size = new LongWritable(size);
  }

  public BlockInfo(long blockid, long blockgenerationstamp, long size, short replication) {
    this.blockId = new LongWritable(blockid);
    this.blockGenerationStamp = new LongWritable(blockgenerationstamp);
    this.size = new LongWritable(size);
    this.replication = replication;
  }

  public void write(DataOutput dataOutput)
      throws IOException {
    blockId.write(dataOutput);
    blockGenerationStamp.write(dataOutput);
    size.write(dataOutput);
  }

  public void readFields(DataInput dataInput)
      throws IOException {
    blockId.readFields(dataInput);
    blockGenerationStamp.readFields(dataInput);
    size.readFields(dataInput);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof BlockInfo)) {
      return false;
    }
    BlockInfo blkInfo = (BlockInfo) o;
    return blkInfo.getBlockId().equals(this.getBlockId())
        && blkInfo.getBlockGenerationStamp().equals(this.getBlockGenerationStamp())
        && blkInfo.getSize().equals(this.getSize());
  }

  @Override
  public int hashCode() {
    return blockId.hashCode() + 357 * blockGenerationStamp.hashCode() + 9357 * size.hashCode();
  }
}
