/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator.audit;

import com.google.common.base.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestAuditLogDirectParser {

  private static final long START_TIMESTAMP = 10000;
  private static final Function<Long, Long> IDENTITY_FN = new Function<Long, Long>() {
    @Override
    public Long apply(Long in) {
      return in;
    }
  };
  private AuditLogDirectParser parser;

  @Before
  public void setup() throws Exception {
    parser = new AuditLogDirectParser();
    Configuration conf = new Configuration();
    conf.setLong(AuditLogDirectParser.AUDIT_START_TIMESTAMP_KEY, START_TIMESTAMP);
    parser.initialize(conf);
  }

  private Text getAuditString(String timestamp, String ugi, String cmd, String src, String dst) {
    return new Text(String.format("%s INFO FSNamesystem.audit: allowed=true\tugi=%s\tip=0.0.0.0\tcmd=%s\tsrc=%s\t" +
        "dst=%s\tperm=null\tproto=rpc", timestamp, ugi, cmd, src, dst));
  }

  @Test
  public void testSimpleInput() throws Exception {
    Text in = getAuditString("1970-01-01 00:00:11,000", "fakeUser", "listStatus", "sourcePath", "null");
    AuditReplayCommand expected =
        new AuditReplayCommand(1000, "listStatus", "sourcePath", "null", "0.0.0.0");
    assertEquals(expected, parser.parse(in, IDENTITY_FN));
  }

  @Test
  public void testInputWithRenameOptions() throws Exception {
    Text in = getAuditString("1970-01-01 00:00:11,000", "fakeUser", "rename (options=[TO_TRASH])",
        "sourcePath", "destPath");
    AuditReplayCommand expected =
        new AuditReplayCommand(1000, "rename (options=[TO_TRASH])", "sourcePath", "destPath", "0.0.0.0");
    assertEquals(expected, parser.parse(in, IDENTITY_FN));
  }

  @Test
  public void testInputWithTokenAuth() throws Exception {
    Text in = getAuditString("1970-01-01 00:00:11,000", "fakeUser (auth:TOKEN)", "create",
        "sourcePath", "null");
    AuditReplayCommand expected =
        new AuditReplayCommand(1000, "create", "sourcePath", "null", "0.0.0.0");
    assertEquals(expected, parser.parse(in, IDENTITY_FN));
  }

  @Test
  public void testInputWithProxyUser() throws Exception {
    Text in = getAuditString("1970-01-01 00:00:11,000", "proxyUser (auth:TOKEN) via fakeUser", "create",
        "sourcePath", "null");
    AuditReplayCommand expected =
        new AuditReplayCommand(1000, "create", "sourcePath", "null", "0.0.0.0");
    assertEquals(expected, parser.parse(in, IDENTITY_FN));
  }

}
