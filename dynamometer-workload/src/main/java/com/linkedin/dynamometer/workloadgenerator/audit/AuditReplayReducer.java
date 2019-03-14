/*
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator.audit;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * AuditReplayReducer aggregates the returned latency values from {@link AuditReplayMapper} and sums
 * them up by {@link UserCommandKey}, which combines the user's id that ran the command and the type
 * of the command (READ/WRITE).
 */
public class AuditReplayReducer extends
    Reducer<UserCommandKey, CountTimeWritable, UserCommandKey, CountTimeWritable> {

  @Override
  protected void reduce(UserCommandKey key, Iterable<CountTimeWritable> values, Context context) throws IOException, InterruptedException {
    long countSum = 0;
    long timeSum = 0;
    for (CountTimeWritable v : values) {
      countSum += v.getCount();
      timeSum += v.getTime();
    }
    context.write(key, new CountTimeWritable(countSum, timeSum));
  }
}
