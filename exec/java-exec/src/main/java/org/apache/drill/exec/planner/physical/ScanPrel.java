/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.store.StoragePlugin;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;

public class ScanPrel extends DrillScanRelBase implements Prel{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanPrel.class);

  public ScanPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable tbl) {
    super(DRILL_PHYSICAL, cluster, traits, tbl);
  }


  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return super.copy(traitSet, inputs);
  }


  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }


  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    StoragePlugin plugin = this.drillTable.getPlugin();
    GroupScan scan = plugin.getPhysicalScan(new JSONOptions(drillTable.getSelection()));

    return scan;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner); 
    }
   
    double rowCount = RelMetadataQuery.getRowCount(this);
    double cpuCost = rowCount; // for now, assume cpu cost is proportional to row count. 
    // Even though scan is reading from disk, in the currently generated plans all plans will
    // need to read the same amount of data, so keeping the disk io cost 0 is ok for now.  
    // In the future we might consider alternative scans that go against projections or 
    // different compression schemes etc that affect the amount of data read. Such alternatives
    // would affect both cpu and io cost. 
    double ioCost = 0;
    DrillCostFactory costFactory = (DrillCostFactory)planner.getCostFactory();
    return costFactory.makeCost(rowCount, cpuCost, ioCost, 0);   
  }
  
}
