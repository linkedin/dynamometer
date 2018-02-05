/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Represents the base class for a generic workload-generating mapper. By default, it will expect to use
 * {@link VirtualInputFormat} as its {@link InputFormat}. Subclasses expecting a different {@link InputFormat}
 * should override the {@link #getInputFormat(Configuration)} method.
 */
public abstract class WorkloadMapper<KEYIN, VALUEIN> extends Mapper<KEYIN, VALUEIN, NullWritable, NullWritable> {

  /**
   * Return the input class to be used by this mapper.
   */
  public Class<? extends InputFormat> getInputFormat(Configuration conf) {
    return VirtualInputFormat.class;
  }

  /**
   * Get the description of the behavior of this mapper.
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
