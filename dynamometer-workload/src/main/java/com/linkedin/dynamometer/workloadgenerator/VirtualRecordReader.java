/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class VirtualRecordReader<K, V> extends RecordReader<K, V> {
  int durationMs;
  long startTimestampInMs;
  long endTimestampInMs;
  static int numRows = 1;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    durationMs = conf.getInt(CreateFileMapper.DURATION_MIN_KEY, 0) * 60 * 1000;
    startTimestampInMs = conf.getInt(WorkloadDriver.START_TIMESTAMP_MS, 0);
    endTimestampInMs = startTimestampInMs + durationMs;
  }

  // The map function per split should be invoked only once.
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (numRows > 0) {
      numRows--;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return (K) NullWritable.get();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return (V) NullWritable.get();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    long remainingMs = endTimestampInMs - System.currentTimeMillis();
    float progress = (remainingMs * 100) / durationMs;
    return progress;
  }

  @Override
  public void close() throws IOException {
    // do Nothing
  }
};
