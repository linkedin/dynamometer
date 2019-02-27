/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


/**
 * An {@link InputFormat} which is time-based. Starts at some timestamp (specified by the
 * {@value WorkloadDriver#START_TIMESTAMP_MS} configuration) and runs for a time specified by the
 * {@value DURATION_KEY} configuration. Spawns {@value NUM_MAPPERS_KEY} mappers. Both {@value DURATION_KEY}
 * and {@value NUM_MAPPERS_KEY} are required.
 *
 * <p>The values returned as the key by this InputFormat are just a sequential counter.
 */
public class TimedInputFormat extends InputFormat<LongWritable, NullWritable> {

  public static final String DURATION_KEY = "timedinput.duration";
  public static final String NUM_MAPPERS_KEY = "timedinput.num-mappers";

  // Number of splits = Number of mappers. Creates fakeSplits to launch
  // the required number of mappers
  @Override
  public List<InputSplit> getSplits(JobContext job) {
    Configuration conf = job.getConfiguration();
    int numMappers = conf.getInt(NUM_MAPPERS_KEY, -1);
    List<InputSplit> splits = new ArrayList<>();
    for (int i = 0; i < numMappers; i++) {
      splits.add(new VirtualInputSplit());
    }
    return splits;
  }

  @Override
  public RecordReader<LongWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new TimedRecordReader();
  }

  public static List<String> getConfigDescriptions() {
    return Lists.newArrayList(
        NUM_MAPPERS_KEY + " (required): Number of mappers to launch.",
        DURATION_KEY + " (required): Number of minutes to induce workload for."
    );
  }

  public static boolean verifyConfigurations(Configuration conf) {
    return conf.getInt(NUM_MAPPERS_KEY, -1) != -1
        && conf.getTimeDuration(DURATION_KEY, -1, TimeUnit.MILLISECONDS) != -1;
  }

}
