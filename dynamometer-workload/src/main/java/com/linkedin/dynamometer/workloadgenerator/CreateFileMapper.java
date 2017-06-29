/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>CreateFileMapper continuously creates 1 byte files for the specified duration to increase the
 * number of file objects on the NN. This uses {@link TimedInputFormat}; see its Javadoc for configuration
 * information.
 *
 * <p>Configuration options available:
 * <ul>
 *   <li>{@value SHOULD_DELETE_KEY} (default: {@value SHOULD_DELETE_DEFAULT}): If true, delete the files
 *       after creating them. This can be useful for generating constant load without increasing the
 *       number of file objects.</li>
 *   <li>{@value FILE_PARENT_PATH_KEY} (default: {@value FILE_PARENT_PATH_DEFAULT}): The root directory
 *       in which to create files.</li>
 * </ul>
 */
public class CreateFileMapper extends WorkloadMapper<LongWritable, NullWritable> {

  public static final String FILE_PARENT_PATH_KEY = "createfile.file-parent-path";
  public static final String FILE_PARENT_PATH_DEFAULT = "/tmp/createFileMapper";
  public static final String SHOULD_DELETE_KEY = "createfile.should-delete";
  public static final boolean SHOULD_DELETE_DEFAULT = false;

  public enum CREATEFILECOUNTERS {
    NUMFILESCREATED
  }

  private static final Logger LOG = LoggerFactory.getLogger(CreateFileMapper.class);
  private static final byte[] FILE_CONTENT = { 0x0 };

  private FileSystem fs;
  private Path filePrefix;
  private boolean shouldDelete;

  @Override
  public String getDescription() {
    return "This mapper creates 1-byte files for the specified duration.";
  }

  @Override
  public List<String> getConfigDescriptions() {
    List<String> baseList = TimedInputFormat.getConfigDescriptions();
    baseList.add(SHOULD_DELETE_KEY + " (default: " + SHOULD_DELETE_DEFAULT +
        "): If true, delete the files after creating them. This can be useful for generating constant load " +
        "without increasing the number of file objects.");
    baseList.add(FILE_PARENT_PATH_KEY + " (default: " + FILE_PARENT_PATH_DEFAULT +
        "): The root directory in which to create files.");
    return baseList;
  }

  @Override
  public boolean verifyConfigurations(Configuration conf) {
    return TimedInputFormat.verifyConfigurations(conf);
  }

  @Override
  public void setup(Mapper.Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    String namenodeURI = conf.get(WorkloadDriver.NN_URI);
    fs = FileSystem.get(URI.create(namenodeURI), conf);
    shouldDelete = conf.getBoolean(SHOULD_DELETE_KEY, SHOULD_DELETE_DEFAULT);
    String fileParentPath = conf.get(FILE_PARENT_PATH_KEY, FILE_PARENT_PATH_DEFAULT);
    int taskID = context.getTaskAttemptID().getTaskID().getId();
    filePrefix = new Path(String.format("%s/mapper%s/file", fileParentPath, taskID));
    LOG.info("Mapper path prefix: " + filePrefix);
  }

  @Override
  public void map(LongWritable key, NullWritable value, Mapper.Context mapperContext) throws IOException {
    Path path = filePrefix.suffix(key.get() + "");
    OutputStream out = fs.create(path);
    out.write(FILE_CONTENT);
    out.close();
    mapperContext.getCounter(CREATEFILECOUNTERS.NUMFILESCREATED).increment(1L);
    if (shouldDelete) {
      fs.delete(path, true);
    }
  }
}
