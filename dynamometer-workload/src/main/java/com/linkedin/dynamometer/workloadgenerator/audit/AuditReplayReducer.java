package com.linkedin.dynamometer.workloadgenerator.audit;

import com.google.common.collect.Lists;
import com.linkedin.dynamometer.workloadgenerator.WorkloadReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.List;

public class AuditReplayReducer extends
    WorkloadReducer<Text, LongWritable, Text, LongWritable> {
  public static final String OUTPUT_PATH_KEY = "auditreplay.output-path";

  @Override
  public Class<? extends OutputFormat> getOutputFormat(Configuration conf) {
    return AuditTextOutputFormat.class;
  }

  @Override
  public boolean verifyConfigurations(Configuration conf) {
    return conf.get(OUTPUT_PATH_KEY) != null;
  }

  @Override
  protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    long sum = 0;
    for (LongWritable v : values) {
      sum += v.get();
    }
    context.write(key, new LongWritable(sum));
  }
}
