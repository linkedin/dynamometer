/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer;

import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestDynoInfraUtils {

  private static final Log LOG = LogFactory.getLog(TestDynoInfraUtils.class);

  @Test
  public void testParseStaleDatanodeListSingleDatanode() throws Exception {
    // Confirm all types of values can be properly parsed
    String json = "{" +
        "\"1.2.3.4:5\": {" +
        "  \"numBlocks\": 5," +
        "  \"fooString\":\"stringValue\"," +
        "  \"fooInteger\": 1," +
        "  \"fooFloat\": 1.0," +
        "  \"fooArray\": []" +
        "}" +
        "}";
    Set<String> out = DynoInfraUtils.parseStaleDataNodeList(json, 10, LOG);
    assertEquals(1, out.size());
    assertTrue(out.contains("1.2.3.4:5"));
  }

  @Test
  public void testParseStaleDatanodeListMultipleDatanodes() throws Exception {
    String json = "{" +
        "\"1.2.3.4:1\": {\"numBlocks\": 0}, " +
        "\"1.2.3.4:2\": {\"numBlocks\": 15}, " +
        "\"1.2.3.4:3\": {\"numBlocks\": 5}, " +
        "\"1.2.3.4:4\": {\"numBlocks\": 10} " +
        "}";
    Set<String> out = DynoInfraUtils.parseStaleDataNodeList(json, 10, LOG);
    assertEquals(2, out.size());
    assertTrue(out.contains("1.2.3.4:1"));
    assertTrue(out.contains("1.2.3.4:3"));
  }

}

