/*
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator.audit;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * UserCommandKey is a {@link Writable} used as a composite value that accumulates the count
 * and cumulative latency of replayed commands. It is used as the output value for
 * AuditReplayMapper and AuditReplayReducer.
 */
public class CountTimeWritable implements Writable {
  private LongWritable count;
  private LongWritable time;

  public CountTimeWritable() {
    count = new LongWritable();
    time = new LongWritable();
  }

  public CountTimeWritable(LongWritable count, LongWritable time) {
    this.count = count;
    this.time = time;
  }

  public CountTimeWritable(long count, long time) {
    this.count = new LongWritable(count);
    this.time = new LongWritable(time);
  }

  public long getCount() {
    return count.get();
  }

  public long getTime() {
    return time.get();
  }

  public void setCount(long count) {
    this.count.set(getCount() + count);
  }

  public void setTime(long time) {
    this.time.set(getTime() + time);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    count.write(out);
    time.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    count.readFields(in);
    time.readFields(in);
  }

  @Override
  public String toString() {
    return getCount() + "," + getTime();
  }
}
