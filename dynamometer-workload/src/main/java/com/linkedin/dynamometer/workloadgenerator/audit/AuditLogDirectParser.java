/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator.audit;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;


/**
 * This {@link AuditCommandParser} is used to read commands from an audit log in the
 * original format audit logs are produced in with a standard configuration. It
 * requires setting the {@value AUDIT_START_TIMESTAMP_KEY} configuration to specify
 * what the start time of the audit log was to determine when events occurred
 * relative to this start time.
 */
public class AuditLogDirectParser implements AuditCommandParser {

  public static final String AUDIT_START_TIMESTAMP_KEY = "auditreplay.log-start-time.ms";

  private static final Pattern MESSAGE_ONLY_PATTERN = Pattern.compile("^([0-9-]+ [0-9:,]+) [^:]+: (.+)$");
  private static final Splitter.MapSplitter AUDIT_SPLITTER =
      Splitter.on("\t").trimResults().omitEmptyStrings().withKeyValueSeparator("=");
  private static final Splitter SPACE_SPLITTER = Splitter.on(" ").trimResults().omitEmptyStrings();
  private static final DateFormat AUDIT_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
  static {
    AUDIT_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  private long startTimestamp;

  @Override
  public void initialize(Configuration conf) throws IOException {
    startTimestamp = conf.getLong(AUDIT_START_TIMESTAMP_KEY, -1);
    if (startTimestamp < 0) {
      throw new IOException("Invalid or missing audit start timestamp: " + startTimestamp);
    }
  }

  @Override
  public AuditReplayCommand parse(Text inputLine, Function<Long, Long> relativeToAbsolute) throws IOException {
    Map<String, String> parameterMap = getParameterMap(inputLine);
    return new AuditReplayCommand(relativeToAbsolute.apply(Long.valueOf(parameterMap.get("relativeTimestamp"))),
            // Split the UGI on space to remove the auth and proxy portions of it
            SPACE_SPLITTER.split(parameterMap.get("ugi")).iterator().next(),
            parameterMap.get("cmd").replace("(options:", "(options="),
            parameterMap.get("src"), parameterMap.get("dst"), parameterMap.get("ip"));
  }

  @Override
  public AuditReplayCommand parse(
          Text inputLine,
          Function<String, Boolean> isTargetUgi,
          Function<Long, Long> relativeToAbsolute,
          Function<Long, Long> ugiRelativeToAbsolute
  ) throws IOException {
    Map<String, String> parameterMap = getParameterMap(inputLine);
    String ugi = SPACE_SPLITTER.split(parameterMap.get("ugi")).iterator().next();
    long relativeTimestamp = Long.valueOf(parameterMap.get("relativeTimestamp"));
    long absoluteTimestamp = isTargetUgi.apply(ugi)
            ? ugiRelativeToAbsolute.apply(relativeTimestamp) : relativeToAbsolute.apply(relativeTimestamp);
    return new AuditReplayCommand(absoluteTimestamp,
            // Split the UGI on space to remove the auth and proxy portions of it
            ugi,
            parameterMap.get("cmd").replace("(options:", "(options="),
            parameterMap.get("src"), parameterMap.get("dst"), parameterMap.get("ip"));
  }

  private Map<String, String> getParameterMap(Text inputLine) throws IOException {
    Matcher m = MESSAGE_ONLY_PATTERN.matcher(inputLine.toString());
    if (!m.find()) {
      throw new IOException("Unable to find valid message pattern from audit log line: " + inputLine);
    }
    long relativeTimestamp;
    try {
      relativeTimestamp = AUDIT_DATE_FORMAT.parse(m.group(1)).getTime() - startTimestamp;
    } catch (ParseException p) {
      throw new IOException("Exception while parsing timestamp from audit log", p);
    }
    // We sanitize the = in the rename options field into a : so we can split on =
    String auditMessageSanitized = m.group(2).replace("(options=", "(options:");
    Map<String, String> parameterMap = AUDIT_SPLITTER.split(auditMessageSanitized);
    parameterMap.put("relativeTimestamp", Long.toString(relativeTimestamp));

    return parameterMap;
  }
}
