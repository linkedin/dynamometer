/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementStatus;


/**
 * A BlockPlacementPolicy which always considered itself satisfied. This avoids the issue that the Dynamometer
 * NameNode will complain about blocks being under-replicated because they're not being put on distinct racks.
 */
public class BlockPlacementPolicyAlwaysSatisfied extends BlockPlacementPolicyDefault {

  private static final BlockPlacementStatusSatisfied SATISFIED = new BlockPlacementStatusSatisfied();

  private static class BlockPlacementStatusSatisfied implements BlockPlacementStatus {
    @Override
    public boolean isPlacementPolicySatisfied() {
      return true;
    }

    public String getErrorDescription() {
      return null;
    }
  }

  @Override
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs, int numberOfReplicas) {
    return SATISFIED;
  }

}
