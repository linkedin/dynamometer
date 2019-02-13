package com.linkedin.dynamometer.workloadgenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.util.List;

public abstract class WorkloadReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  public Class<? extends OutputFormat> getOutputFormat(Configuration conf) {
    return NullOutputFormat.class;
  }

  public abstract boolean verifyConfigurations(Configuration conf);
}
