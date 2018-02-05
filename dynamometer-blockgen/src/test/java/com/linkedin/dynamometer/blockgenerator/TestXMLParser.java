/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.blockgenerator;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestXMLParser {

  /**
   * Testing whether {@link XMLParser} correctly parses an XML fsimage
   * file into {@link BlockInfo}'s. Note that some files have multiple lines.
   */
  @Test
  public void testBlocksFromLine() throws Exception {
    String[] lines = {
        "<INodeSection><lastInodeId>1"
            + "</lastInodeId><inode><id>2</id><type>FILE</type>"
            + "<name>fake-file</name>"
            + "<replication>3</replication><mtime>3</mtime>"
            + "<atime>4</atime>"
            + "<perferredBlockSize>5</perferredBlockSize>"
            + "<permission>hdfs:hdfs:rw-------</permission>"
            + "<blocks><block><id>6</id><genstamp>7</genstamp>"
            + "<numBytes>8</numBytes></block>"
            + "<block><id>9</id><genstamp>10</genstamp>"
            + "<numBytes>11</numBytes></block></inode>",
        "<inode><type>DIRECTORY</type></inode>",
        "<inode><type>FILE</type>",
        "<replication>12</replication>",
        "<blocks><block><id>13</id><genstamp>14</genstamp><numBytes>15</numBytes></block>",
        "</inode>"
    };

    Map<BlockInfo, Short> expectedBlockCount = new HashMap<>();
    expectedBlockCount.put(new BlockInfo(6, 7, 8), (short) 3);
    expectedBlockCount.put(new BlockInfo(9, 10, 11), (short) 3);
    expectedBlockCount.put(new BlockInfo(13, 14, 15), (short) 12);

    final Map<BlockInfo, Short> actualBlockCount = new HashMap<>();
    XMLParser parser = new XMLParser();
    for (String line : lines) {
      for (BlockInfo info : parser.parseLine(line)) {
        actualBlockCount.put(info, info.getReplication());
      }
    }

    for (Map.Entry<BlockInfo, Short> expect : expectedBlockCount.entrySet()) {
      assertEquals(expect.getValue(), actualBlockCount.get(expect.getKey()));
    }
  }
}
