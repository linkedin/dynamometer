/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.workloadgenerator.audit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class UserCommandKey implements WritableComparable {
  private Text user;
  private Text command;

  public UserCommandKey() {
    user = new Text();
    command = new Text();
  }

  public UserCommandKey(Text user, Text command) {
    this.user = user;
    this.command = command;
  }

  public UserCommandKey(String user, String command) {
    this.user = new Text(user);
    this.command = new Text(command);
  }

  public String getUser() {
    return user.toString();
  }

  public void setUser(String user) {
    this.user.set(user);
  }

  public String getCommand() {
    return command.toString();
  }

  public void setCommand(String command) {
    this.command.set(command);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    user.write(out);
    command.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    user.readFields(in);
    command.readFields(in);
  }

  @Override
  public int compareTo(@Nonnull Object o) {
    return Integer.compare(hashCode(), o.hashCode());
  }

  @Override
  public String toString() {
    return getUser() + "," + getCommand();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UserCommandKey that = (UserCommandKey) o;
    return getUser().equals(that.getUser()) && getCommand().equals(that.getCommand());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getUser(), getCommand());
  }
}
