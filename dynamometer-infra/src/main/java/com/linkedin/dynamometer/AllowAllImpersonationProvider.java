/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ImpersonationProvider;


/**
 * An {@link ImpersonationProvider} that indiscriminately allows all users
 * to proxy as any other user.
 */
public class AllowAllImpersonationProvider extends Configured implements ImpersonationProvider {

  public void init(String configurationPrefix) {
    // Do nothing
  }
  public void authorize(UserGroupInformation user, String remoteAddress) {
    // Do nothing
  }

}
