/*
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator.audit;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * <p>AuditReplayReducer aggregates the returned latency values from {@link AuditReplayMapper} and sums
 * them up by {@link UserCommandKey}, which combines the user's id that ran the command and the type
 * of the command (READ/WRITE).
 */
public class AuditReplayReducer extends
    Reducer<UserCommandKey, LongWritable, UserCommandKey, LongWritable> {

  @Override
  protected void reduce(UserCommandKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    long sum = 0;
    for (LongWritable v : values) {
      sum += v.get();
    }
    context.write(key, new LongWritable(sum));
  }
}
