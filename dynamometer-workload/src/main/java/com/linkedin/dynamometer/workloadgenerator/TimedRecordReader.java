/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.dynamometer.workloadgenerator.TimedInputFormat.DURATION_KEY;


/**
 * A {@link RecordReader} to support {@link TimedInputFormat}. Waits to emit the first
 * value until the start time has been reached, then continually emits values
 * (a sequential counter) until the end of the duration has been reached.
 */
public class TimedRecordReader extends RecordReader<LongWritable, NullWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(TimedRecordReader.class);

  private LongWritable nextValue;
  private long durationMs;
  private long startTimestampMs;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) {
    Configuration conf = context.getConfiguration();
    durationMs = conf.getTimeDuration(DURATION_KEY, -1, TimeUnit.MILLISECONDS);
    startTimestampMs = conf.getLong(WorkloadDriver.START_TIMESTAMP_MS, 0);
    nextValue = new LongWritable(0);
  }

  @Override
  public boolean nextKeyValue() throws InterruptedException {
    long elapsed = System.currentTimeMillis() - startTimestampMs;
    LOG.info("Starting at " + startTimestampMs + " ms");
    if (elapsed < 0) {
      LOG.info("Sleeping for " + (-1 * elapsed) + " ms");
      Thread.sleep(-1 * elapsed);
    } else if (elapsed > durationMs) {
      return false;
    }
    nextValue.set(nextValue.get() + 1L);
    return true;
  }

  @Override
  public LongWritable getCurrentKey() {
    return nextValue;
  }

  @Override
  public NullWritable getCurrentValue() {
    return NullWritable.get();
  }

  @Override
  public float getProgress() {
    return (System.currentTimeMillis() - startTimestampMs) / ((float) durationMs);
  }

  @Override
  public void close() {
    // do Nothing
  }
}
