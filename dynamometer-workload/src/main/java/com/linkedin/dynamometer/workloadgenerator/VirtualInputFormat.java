/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class VirtualInputFormat<K, V> extends FileInputFormat<K, V>
{
  // Number of splits = Number of mappers. Creates fakeSplits to launch
  // the required number of mappers
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException
  {
    Configuration conf = job.getConfiguration();
    int numMappers = conf.getInt(CreateFileMapper.NUM_MAPPERS_KEY, -1);
    if (numMappers == -1)
      throw new IOException("Number of mappers should be provided as input");
    List<InputSplit> splits = new ArrayList<InputSplit>(numMappers);
    for (int i = 0; i < numMappers; i++)
      splits.add(new VirtualInputSplit<K, V>());
    return splits;
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException
  {
    return new VirtualRecordReader<K, V>();
  }
}
