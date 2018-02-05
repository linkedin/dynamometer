/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer;

import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceType;


class DynoResource {

  private final String name;
  private final LocalResourceType type;
  private final String resourcePath;

  DynoResource(String name, LocalResourceType type, String resourcePath) {
    this.name = name;
    this.type = type;
    this.resourcePath = resourcePath;
  }

  public Path getPath(Map<String, String> env) {
    return new Path(env.get(getLocationEnvVar()));
  }

  public long getTimestamp(Map<String, String> env) {
    return Long.parseLong(env.get(getTimestampEnvVar()));
  }

  public long getLength(Map<String, String> env) {
    return Long.parseLong(env.get(getLengthEnvVar()));
  }

  public String getLocationEnvVar() {
    return name + "_LOCATION";
  }

  public String getTimestampEnvVar() {
    return name + "_TIMESTAMP";
  }

  public String getLengthEnvVar() {
    return name + "_LENGTH";
  }

  public LocalResourceType getType() {
    return type;
  }

  public String getResourcePath() {
    return resourcePath;
  }

  public String toString() {
    return name;
  }

}
