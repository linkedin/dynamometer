/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


/**
 * AbusiveUserMapper consists of two types of users: one type that performs a large listing operation on a directory
 * with many files, and another type that performs a small operation (either a single file listing or a single directory
 * creation). This uses {@link TimedInputFormat}; see its Javadoc for configuration information. Only requires the
 * NameNode, does not make file data changes.
 *
 * <p>Configuration options available:
 * <ul>
 *   <li>{@value FILE_PARENT_PATH_KEY} (default: {@value FILE_PARENT_PATH_DEFAULT}): The root directory for the job to
 *   create files in.</li>
 *   <li>{@value NUM_LARGE_JOBS_KEY} (default: {@value NUM_LARGE_JOBS_DEFAULT}): The number of large listing jobs.
 *   Mappers numbered 0 through this value will run the abusive operation.</li>
 *   <li>{@value NUM_FILES_KEY} (default: {@value NUM_FILES_DEFAULT}): Number of files in the directory for the large
 *   listing operations.</li>
 *   <li>{@value MAPPERS_PER_USER_KEY} (default: {@value MAPPERS_PER_USER_DEFAULT}): Number of mappers to assign to each
 *   user. Make a single large/abusive user by setting this and {@value NUM_LARGE_JOBS_KEY} equal.
 *   <li>{@value ENABLE_WRITE_OPS_KEY} (default: {@value ENABLE_WRITE_OPS_DEFAULT}): Enabling this sets the small
 *   operation to create a single directory, instead of listing a single file directory.
 * </ul>
 */
public class AbusiveUserMapper extends WorkloadMapper<LongWritable, NullWritable, NullWritable, NullWritable> {

  public static final String FILE_PARENT_PATH_KEY = "bigfile.file-parent-path";
  public static final String FILE_PARENT_PATH_DEFAULT = "/tmp/createFileMapper";

  public static final String NUM_LARGE_JOBS_KEY = "bigfile.num-large-jobs";
  public static final int NUM_LARGE_JOBS_DEFAULT = 10;

  public static final String NUM_FILES_KEY = "bigfile.num-files";
  public static final int NUM_FILES_DEFAULT = 1000;

  public static final String MAPPERS_PER_USER_KEY = "bigfile.mappers-per-user";
  public static final int MAPPERS_PER_USER_DEFAULT = 10;

  public static final String ENABLE_WRITE_OPS_KEY = "bigfile.enable-write-ops";
  public static final boolean ENABLE_WRITE_OPS_DEFAULT = false;

  private ConcurrentHashMap<String, FileSystem> fsMap = new ConcurrentHashMap<String, FileSystem>();
  private Path parentFolder;
  private int numLargeJobs;
  private int mappersPerUser;
  private boolean enableWriteOps;

  @Override
  public String getDescription() {
    return "This mapper creates a number of large and small operations belonging to different users.";
  }

  @Override
  public List<String> getConfigDescriptions() {
    List<String> baseList = TimedInputFormat.getConfigDescriptions();
    baseList.add(FILE_PARENT_PATH_KEY + " (default: " + FILE_PARENT_PATH_DEFAULT +
        "): The root directory for the job to create files in.");
    baseList.add(NUM_LARGE_JOBS_KEY + " (default: " + NUM_LARGE_JOBS_DEFAULT +
        "): Number of large listing jobs.");
    baseList.add(NUM_FILES_KEY + " (default: " + NUM_FILES_DEFAULT +
        "): Number of files in the directory for the large listing operations.");
    baseList.add(MAPPERS_PER_USER_KEY + " (default: " + MAPPERS_PER_USER_DEFAULT +
        "): Number of mappers per user.");
    baseList.add(ENABLE_WRITE_OPS_KEY + " (default: " + ENABLE_WRITE_OPS_DEFAULT +
        "): For the small operation, creates a single directory instead of listing a single file directory.");
    return baseList;
  }

  @Override
  public boolean verifyConfigurations(Configuration conf) {
    return TimedInputFormat.verifyConfigurations(conf);
  }

  @Override
  public void setup(Context context) throws IOException {
    // Load config
    Configuration conf = context.getConfiguration();
    int taskID = context.getTaskAttemptID().getTaskID().getId();
    numLargeJobs = conf.getInt(NUM_LARGE_JOBS_KEY, NUM_LARGE_JOBS_DEFAULT);
    int numFiles = conf.getInt(NUM_FILES_KEY, NUM_FILES_DEFAULT);
    mappersPerUser = conf.getInt(MAPPERS_PER_USER_KEY, MAPPERS_PER_USER_DEFAULT);
    enableWriteOps = conf.getBoolean(ENABLE_WRITE_OPS_KEY, ENABLE_WRITE_OPS_DEFAULT);

    // Load filesystem
    String namenodeURI = conf.get(WorkloadDriver.NN_URI);
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    String proxyUser = "fakeuser" + taskID / mappersPerUser;
    UserGroupInformation ugi = UserGroupInformation.createProxyUser(proxyUser, loginUser);
    FileSystem fs = ugi.doAs(new PrivilegedAction<FileSystem>() {
      @Override
      public FileSystem run() {
        try {
          FileSystem fs = new DistributedFileSystem();
          fs.initialize(URI.create(namenodeURI), conf);
          return fs;
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    });
    fsMap.put(proxyUser, fs);

    // Load default path
    String fileParentPath = conf.get(FILE_PARENT_PATH_KEY, FILE_PARENT_PATH_DEFAULT);
    parentFolder = new Path(new Path(fileParentPath), "mapper" + taskID);

    // Make job folder
    fs.mkdirs(parentFolder);

    if (taskID < numLargeJobs) {
      // Large job: set up files for large listing op
      for (int i = 0; i < numFiles; i++) {
        fs.mkdirs(new Path(parentFolder, new Path("big" + i)));
      }
    } else if (enableWriteOps) {
      // Small write job: no setup required
    } else {
      // Small read job: set up file for single-file listing op
      fs.mkdirs(new Path(parentFolder, new Path("small" + taskID)));
    }
  }

  @Override
  public void map(LongWritable key, NullWritable value, Context mapperContext)
      throws IOException {
    int taskID = mapperContext.getTaskAttemptID().getTaskID().getId();
    String proxyUser = "fakeuser" + taskID / mappersPerUser;
    FileSystem fs = fsMap.get(proxyUser);

    if (taskID < numLargeJobs) {
      // Large job: lists
      fs.listStatus(parentFolder);
    } else if (enableWriteOps) {
      // Small mkdir op
      fs.mkdirs(new Path(parentFolder, new Path("small" + taskID)));
      fs.delete(new Path(parentFolder, new Path("small" + taskID)), true);
    } else {
      // Small listing op
      fs.listStatus(new Path(parentFolder, new Path("small" + taskID)));
    }
  }
}
