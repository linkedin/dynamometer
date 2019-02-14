/*
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator.audit;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AuditReplayReducer extends
    Reducer<Text, LongWritable, Text, LongWritable> {

  @Override
  protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    long sum = 0;
    for (LongWritable v : values) {
      sum += v.get();
    }
    context.write(key, new LongWritable(sum));
  }
}
