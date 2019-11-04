/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.blockgenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class parses an fsimage file in XML format. It accepts the file
 * line-by-line and maintains an internal state machine to keep track of
 * contextual information. A single parser must process the entire file
 * with the lines in the order they appear in the original file.
 *
 * A file may be spread across multiple lines, so we need to track the
 * replication of the file we are currently processing to be aware of
 * what the replication factor is for each block we encounter. This is
 * why we require a single mapper.
 *
 * The format is illustrated below (line breaks for readability):
 * <inode><id>inode_ID<id/>
 *   <type>inode_type</type>
 *   <replication>inode_replication</replication>
 *   [file attributes]
 *   <blocks>
 *     <block><id>XXX</id><genstamp>XXX</genstamp><numBytes>XXX</numBytes><block/>
 *   <blocks/>
 * <inode/>
 *
 * This is true in both Hadoop 2 and 3.
 */
class XMLParser {

  // Legacy block id can be negative
  private static final Pattern BLOCK_PATTERN =
      Pattern.compile("<block><id>(-?\\d+)</id><genstamp>(\\d+)</genstamp><numBytes>(\\d+)</numBytes></block>");

  private State currentState = State.DEFAULT;
  private short currentReplication;

  /**
   * Accept a single line of the XML file, and return a {@link BlockInfo} for any
   * blocks contained within that line. Update internal state dependent on other
   * XML values seen, e.g. the beginning of a file.
   * @param line The XML line to parse.
   * @return {@code BlockInfo}s for any blocks found.
   */
  List<BlockInfo> parseLine(String line) throws IOException {
    if (line.contains("<inode>")) {
      transitionTo(State.INODE);
    }
    if (line.contains("<type>FILE</type>")) {
      transitionTo(State.FILE);
    }
    List<String> replicationStrings = valuesFromXMLString(line, "replication");
    if (!replicationStrings.isEmpty()) {
      if (replicationStrings.size() > 1) {
        throw new IOException(String.format("Found %s replication strings", replicationStrings.size()));
      }
      transitionTo(State.FILE_WITH_REPLICATION);
      currentReplication = Short.parseShort(replicationStrings.get(0));
    }
    Matcher blockMatcher = BLOCK_PATTERN.matcher(line);
    List<BlockInfo> blockInfos = new ArrayList<>();
    while (blockMatcher.find()) {
      if (currentState != State.FILE_WITH_REPLICATION) {
        throw new IOException("Found a block string when in state: " + currentState);
      }
      long id = Long.parseLong(blockMatcher.group(1));
      long gs = Long.parseLong(blockMatcher.group(2));
      long size = Long.parseLong(blockMatcher.group(3));
      blockInfos.add(new BlockInfo(id, gs, size, currentReplication));
    }
    if (line.contains("</inode>")) {
      transitionTo(State.DEFAULT);
    }
    return blockInfos;
  }

  /**
   * Attempt to transition to another state.
   * @param nextState The new state to transition to.
   * @throws IOException If the transition from the current state to {@code nextState}
   *                     is not allowed.
   */
  private void transitionTo(State nextState) throws IOException {
    if (currentState.transitionAllowed(nextState)) {
      currentState = nextState;
    } else {
      throw new IOException("State transition not allowed; from " + currentState + " to " + nextState);
    }
  }

  /**
   * @param xml An XML string
   * @param field The field whose value(s) should be extracted
   * @return List of the field's values.
   */
  private static List<String> valuesFromXMLString(String xml, String field) {
    Matcher m = Pattern.compile("<" + field + ">(.+?)</" + field + ">").matcher(xml);
    List<String> found = new ArrayList<>();
    while (m.find()) {
      found.add(m.group(1));
    }
    return found;
  }

  private enum State {
    DEFAULT,
    INODE,
    FILE,
    FILE_WITH_REPLICATION;

    private final Set<State> allowedTransitions = new HashSet<>();
    static {
      DEFAULT.addTransitions(DEFAULT, INODE);
      INODE.addTransitions(DEFAULT, FILE);
      FILE.addTransitions(DEFAULT, FILE_WITH_REPLICATION);
      FILE_WITH_REPLICATION.addTransitions(DEFAULT);
    }

    private void addTransitions(State... nextState) {
      allowedTransitions.addAll(Arrays.asList(nextState));
    }

    boolean transitionAllowed(State nextState) {
      return allowedTransitions.contains(nextState);
    }
  }

}
