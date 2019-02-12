/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator.audit;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * A simple {@link TextInputFormat} that disables splitting of files. This is the
 * {@link org.apache.hadoop.mapreduce.InputFormat} used by {@link AuditReplayMapper}.
 */
public class NoSplitTextInputFormat extends TextInputFormat {

  @Override
  public List<FileStatus> listStatus(JobContext context) throws IOException {
    context.getConfiguration().set(FileInputFormat.INPUT_DIR,
        context.getConfiguration().get(AuditReplayMapper.INPUT_PATH_KEY));
    return super.listStatus(context);
  }

  @Override
  public boolean isSplitable(JobContext context, Path file) {
    return false;
  }

}
