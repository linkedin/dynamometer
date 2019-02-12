/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator.audit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.List;

public class AuditTextOutputFormat extends TextOutputFormat {
  @Override
  public void checkOutputSpecs(JobContext context) throws IOException {
    context.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir",
        context.getConfiguration().get(AuditReplayReducer.OUTPUT_PATH_KEY));
    super.checkOutputSpecs(context);
  }
}
