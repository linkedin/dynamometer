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

  /**
   * Get the description of the behavior of this reducer.
   */
  public abstract String getDescription();

  /**
   * Get a list of the description of each configuration that this mapper accepts.
   */
  public abstract List<String> getConfigDescriptions();

  /**
   * Verify that the provided configuration contains all configurations
   * required by this mapper.
   */
  public abstract boolean verifyConfigurations(Configuration conf);
}
