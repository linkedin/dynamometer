/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator.audit;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.base.Function;
import com.linkedin.dynamometer.workloadgenerator.WorkloadDriver;
import com.linkedin.dynamometer.workloadgenerator.WorkloadMapper;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import static com.linkedin.dynamometer.workloadgenerator.audit.AuditReplayMapper.CommandType.READ;
import static com.linkedin.dynamometer.workloadgenerator.audit.AuditReplayMapper.CommandType.WRITE;


/**
 * <p>AuditReplayMapper replays the given audit trace against the NameNode under test. Each mapper
 * spawns a number of threads equal to the {@value NUM_THREADS_KEY} configuration (by default
 * {@value NUM_THREADS_DEFAULT}) to use for replaying. Each mapper reads a single input file
 * which will be consumed by all of the available threads. A {@link FileInputFormat} with
 * splitting disabled is used so any files present in the input path directory (given by the
 * {@value INPUT_PATH_KEY} configuration) will be used as input; one file per mapper. The expected
 * format of these files is determined by the value of the {@value COMMAND_PARSER_KEY} configuration,
 * which defaults to {@link AuditLogDirectParser}.
 *
 * <p>This generates a number of {@link org.apache.hadoop.mapreduce.Counter} values which can be used to
 * get information into the replay, including the number of commands replayed, how many of them were
 * "invalid" (threw an exception), how many were "late" (replayed later than they should have been),
 * and the latency (from client perspective) of each command. If there are a large number of "late"
 * commands, you likely need to increase the number of threads used and/or the number of mappers.
 *
 * By default, commands will be replayed at the same rate as they were originally performed. However
 * a rate factor can be specified via the {@value RATE_FACTOR_KEY} configuration; all of the (relative)
 * timestamps will be divided by this rate factor, effectively changing the rate at which they are
 * replayed. For example, a rate factor of 2 would make the replay occur twice as fast, and a rate
 * factor of 0.5 would make it occur half as fast.
 */
public class AuditReplayMapper extends WorkloadMapper<LongWritable, Text> {

  public static final String INPUT_PATH_KEY = "auditreplay.input-path";
  public static final String NUM_THREADS_KEY = "auditreplay.num-threads";
  public static final int NUM_THREADS_DEFAULT = 1;
  public static final String CREATE_BLOCKS_KEY = "auditreplay.create-blocks";
  public static final boolean CREATE_BLOCKS_DEFAULT = true;
  public static final String RATE_FACTOR_KEY = "auditreplay.rate-factor";
  public static final double RATE_FACTOR_DEFAULT = 1.0;
  public static final String COMMAND_PARSER_KEY = "auditreplay.command-parser.class";
  public static final Class<AuditLogDirectParser> COMMAND_PARSER_DEFAULT = AuditLogDirectParser.class;

  private static final Log LOG = LogFactory.getLog(AuditReplayMapper.class);
  // This is the maximum amount that the mapper should read ahead from the input
  // as compared to the replay time. Setting this to one minute avoids reading too
  // many entries into memory simultaneously but ensures that the replay threads
  // should not ever run out of entries to replay.
  private static final long MAX_READAHEAD_MS = 60000;

  public static final String INDIVIDUAL_COMMANDS_COUNTER_GROUP = "INDIVIDUAL_COMMANDS";
  public static final String INDIVIDUAL_COMMANDS_LATENCY_SUFFIX = "_LATENCY";
  public static final String INDIVIDUAL_COMMANDS_INVALID_SUFFIX = "_INVALID";
  public static final String INDIVIDUAL_COMMANDS_COUNT_SUFFIX = "_COUNT";

  public enum REPLAYCOUNTERS {
    // Total number of commands that were replayed
    TOTALCOMMANDS,
    // Total number of commands that returned an error during replay (incl unsupported)
    TOTALINVALIDCOMMANDS,
    // Total number of commands that are unsupported for replay
    TOTALUNSUPPORTEDCOMMANDS,
    // Total number of commands that were performed later than they should have been
    LATECOMMANDS,
    // Total delay time of all commands that were performed later than they should have been
    LATECOMMANDSTOTALTIME,
    // Total number of write operations
    TOTALWRITECOMMANDS,
    // Total latency for all write operations
    TOTALWRITECOMMANDLATENCY,
    // Total number of read operations
    TOTALREADCOMMANDS,
    // Total latency for all read operations
    TOTALREADCOMMANDLATENCY
  }

  public enum ReplayCommand {
    APPEND(WRITE),
    CREATE(WRITE),
    GETFILEINFO(READ),
    CONTENTSUMMARY(READ),
    MKDIRS(WRITE),
    RENAME(WRITE),
    LISTSTATUS(READ),
    DELETE(WRITE),
    OPEN(READ),
    SETPERMISSION(WRITE),
    SETOWNER(WRITE),
    SETTIMES(WRITE),
    SETREPLICATION(WRITE),
    CONCAT(WRITE);

    private final CommandType type;

    ReplayCommand(CommandType type) {
      this.type = type;
    }

    public CommandType getType() {
      return type;
    }
  }

  public enum CommandType {
    READ, WRITE
  }

  private long startTimestampMs;
  private int numThreads;
  private double rateFactor;
  private long highestTimestamp;
  private List<AuditReplayThread> threads;
  private Function<Long, Long> relativeToAbsoluteTimestamp;
  private AuditCommandParser commandParser;

  @Override
  public Class<? extends InputFormat> getInputFormat(Configuration conf) {
    return NoSplitTextInputFormat.class;
  }

  @Override
  public String getDescription() {
    return "This mapper replays audit log files.";
  }

  @Override
  public List<String> getConfigDescriptions() {
    return Lists.newArrayList(
        INPUT_PATH_KEY + " (required): Path to directory containing input files.",
        NUM_THREADS_KEY + " (default " + NUM_THREADS_DEFAULT + "): Number of threads to use per mapper for replay.",
        CREATE_BLOCKS_KEY + " (default " + CREATE_BLOCKS_DEFAULT + "): Whether or not to create 1-byte blocks when " +
            "performing `create` commands.",
        RATE_FACTOR_KEY + " (default " + RATE_FACTOR_DEFAULT + "): Multiplicative speed at which to replay the audit " +
            " log; e.g. a value of 2.0 would make the replay occur at twice the original speed. This can be useful " +
            "to induce heavier loads."
    );
  }

  @Override
  public boolean verifyConfigurations(Configuration conf) {
    return conf.get(INPUT_PATH_KEY) != null;
  }

  @Override
  public void setup(Mapper.Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    // WorkloadDriver ensures that the starttimestamp is set
    startTimestampMs = conf.getLong(WorkloadDriver.START_TIMESTAMP_MS, -1);
    numThreads = conf.getInt(NUM_THREADS_KEY, NUM_THREADS_DEFAULT);
    rateFactor = conf.getDouble(RATE_FACTOR_KEY, RATE_FACTOR_DEFAULT);
    try {
      commandParser = conf.getClass(COMMAND_PARSER_KEY, COMMAND_PARSER_DEFAULT, AuditCommandParser.class)
          .getConstructor().newInstance();
    } catch (NoSuchMethodException|InstantiationException|IllegalAccessException|InvocationTargetException e) {
      throw new IOException("Exception encountered while instantiating the command parser", e);
    }
    commandParser.initialize(conf);
    relativeToAbsoluteTimestamp = new Function<Long, Long>() {
      @Override
      public Long apply(Long input) {
        return startTimestampMs + Math.round(input / rateFactor);
      }
    };

    LOG.info("Starting " + numThreads + " threads");

    threads = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      AuditReplayThread thread = new AuditReplayThread(context);
      threads.add(thread);
      thread.start();
    }
  }

  @Override
  public void map(LongWritable lineNum, Text inputLine, Mapper.Context context)
      throws IOException, InterruptedException {
    AuditReplayCommand cmd = commandParser.parse(inputLine, relativeToAbsoluteTimestamp);
    long delay = cmd.getDelay(TimeUnit.MILLISECONDS);
    // Prevent from loading too many elements into memory all at once
    if (delay > MAX_READAHEAD_MS) {
      Thread.sleep(delay - (MAX_READAHEAD_MS / 2));
    }
    int idx = cmd.getSrc().hashCode() % numThreads;
    if (idx < 0) {
      idx += numThreads;
    }
    threads.get(idx).addToQueue(cmd);
    highestTimestamp = cmd.getAbsoluteTimestamp();
  }

  @Override
  public void cleanup(Mapper.Context context) throws InterruptedException {
    for (AuditReplayThread t : threads) {
      // Add in an indicator for each thread to shut down after the last real command
      t.addToQueue(AuditReplayCommand.getPoisonPill(highestTimestamp + 1));
    }
    Optional<Exception> threadException = Optional.absent();
    for (AuditReplayThread t : threads) {
      t.join();
      t.drainCounters(context);
      if (t.getException() != null) {
        threadException = Optional.of(t.getException());
      }
    }

    if (threadException.isPresent()) {
      throw new RuntimeException("Exception in AuditReplayThread", threadException.get());
    }
    LOG.info("Time taken to replay the logs in ms: " + (System.currentTimeMillis() - startTimestampMs));
    long totalCommands = context.getCounter(REPLAYCOUNTERS.TOTALCOMMANDS).getValue();
    if (totalCommands != 0) {
      float percentageOfInvalidOps =
          (context.getCounter(REPLAYCOUNTERS.TOTALINVALIDCOMMANDS).getValue() * 100) / totalCommands;
      LOG.info("Percentage of invalid ops: " + percentageOfInvalidOps);
    }
  }
}
