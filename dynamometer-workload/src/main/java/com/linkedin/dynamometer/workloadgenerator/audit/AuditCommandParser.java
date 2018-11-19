/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator.audit;

import com.google.common.base.Function;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;


/**
 * This interface represents a pluggable command parser. It will accept in one
 * line of {@link Text} input at a time and return an {@link AuditReplayCommand}
 * which represents the input text. Each input line should produce exactly one command.
 */
public interface AuditCommandParser {

  /**
   * Initialize this parser with the given configuration. Guaranteed to be called
   * prior to any calls to {@link #parse(Text, Function)}.
   * @param conf The Configuration to be used to set up this parser.
   */
  void initialize(Configuration conf) throws IOException;

  /**
   * Convert a line of input into an {@link AuditReplayCommand}.
   * Since {@link AuditReplayCommand}s store absolute timestamps, relativeToAbsolute
   * can be used to convert relative timestamps (i.e., milliseconds elapsed between
   * the start of the audit log and this command) into absolute timestamps.
   * @param inputLine Single input line to convert.
   * @param relativeToAbsolute Function converting relative timestamps (in milliseconds)
   *                           to absolute timestamps (in milliseconds).
   * @return A command representing the input line.
   */
  AuditReplayCommand parse(Text inputLine, Function<Long, Long> relativeToAbsolute) throws IOException;

  /**
   * Convert a line of input into an {@link AuditReplayCommand}.
   * Since {@link AuditReplayCommand}s store absolute timestamps, relativeToAbsolute
   * can be used to convert relative timestamps (i.e., milliseconds elapsed between
   * the start of the audit log and this command) into absolute timestamps.
   * @param inputLine Single input line to convert.
   * @param relativeToAbsolute Function converting relative timestamps (in milliseconds)
   *                           to absolute timestamps (in milliseconds).
   * @param isTargetUgi Function to determine whether the input record is from a target ugi
   * @return A command representing the input line.
   */
  AuditReplayCommand parse(
          Text inputLine,
          Function<String, Boolean> isTargetUgi,
          Function<Long, Long> relativeToAbsolute,
          Function<Long, Long> ugiRelativeToAbsolute
  ) throws IOException;
}
