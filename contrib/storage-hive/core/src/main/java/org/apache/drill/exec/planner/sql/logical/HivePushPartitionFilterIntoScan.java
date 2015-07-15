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

package org.apache.drill.exec.planner.sql.logical;

import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.util.BitSets;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.PartitionDescriptor;
import org.apache.drill.exec.planner.logical.DirPathBuilder;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.PartitionPruningUtil;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.logical.partition.PruneScanRule;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.sql.HivePartitionDescriptor;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.hive.HiveReadEntry;
import org.apache.drill.exec.store.hive.HiveScan;
import org.apache.drill.exec.store.hive.HiveTable;
import org.apache.drill.exec.store.hive.HiveTable.HivePartition;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;

import com.google.common.collect.Lists;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.hive.metastore.api.Partition;

public abstract class HivePushPartitionFilterIntoScan extends StoragePluginOptimizerRule {

  public static final StoragePluginOptimizerRule getFilterOnHive(QueryContext context) {
    return new PruneScanRule(
        RelOptHelper.some(DrillFilterRel.class, RelOptHelper.any(DrillScanRel.class)),
        "HivePushPartitionFilterIntoScan:Filter_On_Scan", context) {

      @Override
      public boolean matches(RelOptRuleCall call) {
        final DrillScanRel scan = (DrillScanRel) call.rel(1);
        GroupScan groupScan = scan.getGroupScan();
        // this rule is applicable only for dfs based partition pruning
        return groupScan.supportsPartitionFilterPushdown();
      }

      @Override
      public void onMatch(RelOptRuleCall call) {
        final DrillFilterRel filterRel = (DrillFilterRel) call.rel(0);
        final DrillScanRel scanRel = (DrillScanRel) call.rel(1);
        doOnMatch(call, filterRel, null, scanRel);
      }

      @Override
      protected PartitionDescriptor getPartitionDescriptor(PlannerSettings settings, DrillScanRel scanRel) {
        HiveReadEntry origReadEntry = ((HiveScan)scanRel.getGroupScan()).hiveReadEntry;
        return new HivePartitionDescriptor(origReadEntry.table.partitionKeys);
      }

      @Override
      protected void populatePartitionVectors(ValueVector[] vectors, List<PathPartition> partitions, BitSet partitionColumnBitSet, Map<Integer, String> fieldNameMap, GroupScan groupScan) {
        int record = 0;
        for(Iterator<PathPartition> iter = partitions.iterator(); iter.hasNext(); record++){
          final PathPartition partition = iter.next();
          for(int partitionColumnIndex : BitSets.toIter(partitionColumnBitSet)){
            if(partition.dirs[partitionColumnIndex] == null){
              ((NullableIntVector) vectors[partitionColumnIndex]).getMutator().setNull(record);
            }else{
              String dir = partition.dirs[partitionColumnIndex];
              dir = dir.substring(dir.indexOf('=') + 1);
              ((NullableIntVector) vectors[partitionColumnIndex]).getMutator().setSafe(record, Integer.parseInt(dir));
            }
          }
        }

        for(ValueVector v : vectors){
          if(v == null){
            continue;
          }
          v.getMutator().setValueCount(partitions.size());
        }
      }


      @Override
      protected TypeProtos.MajorType getVectorType(GroupScan groupScan, SchemaPath column) {
        // TODO FIX LOGIC TO RETURN THE CORRECT TYPE
        return TypeProtos.MajorType.newBuilder().setMode(TypeProtos.DataMode.OPTIONAL).setMinorType(TypeProtos.MinorType.INT).build();
      }

      @Override
      protected List<String> getFiles(DrillScanRel scanRel) {
        HiveReadEntry origEntry = ((HiveScan) scanRel.getGroupScan()).hiveReadEntry;
        List<String> partitionLocations = new LinkedList<>();
        for (Partition partition: origEntry.getPartitions()) {
          partitionLocations.add(partition.getSd().getLocation());
        }
        return partitionLocations;
      }

      @Override
      public String getTableSelectionRoot(DrillScanRel scanRel) {
        HiveReadEntry origEntry = ((HiveScan) scanRel.getGroupScan()).hiveReadEntry;
        return origEntry.table.getTable().getSd().getLocation();
      }

      @Override
      public GroupScan createNewGroupScan(DrillScanRel oldScanRel, List<String> newFiles) throws Exception {
        HiveScan hiveScan = (HiveScan) oldScanRel.getGroupScan();
        HiveReadEntry origReadEntry = hiveScan.hiveReadEntry;
        List<HivePartition> oldPartitions = origReadEntry.partitions;
        List<HivePartition> newPartitions = new LinkedList<>();

        for (HivePartition part: oldPartitions) {
          String partitionLocation = part.getPartition().getSd().getLocation();
          for (String newPartitionLocation: newFiles) {
            if (partitionLocation.equals(newPartitionLocation)) {
              newPartitions.add(part);
            }
          }
        }

        HiveReadEntry newReadEntry = new HiveReadEntry(origReadEntry.table, newPartitions, origReadEntry.hiveConfigOverride);
        HiveScan newScan = new HiveScan(hiveScan.getUserName(), newReadEntry, hiveScan.storagePlugin, hiveScan.columns);
        return newScan;
      }

    };
  }


  public static final StoragePluginOptimizerRule HIVE_FILTER_ON_PROJECT =
      new HivePushPartitionFilterIntoScan(
          RelOptHelper.some(DrillFilterRel.class, RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class))),
          "HivePushPartitionFilterIntoScan:Filter_On_Project") {

        @Override
        public boolean matches(RelOptRuleCall call) {
          final DrillScanRel scan = (DrillScanRel) call.rel(2);
          GroupScan groupScan = scan.getGroupScan();
          return groupScan instanceof HiveScan &&  groupScan.supportsPartitionFilterPushdown();
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
          final DrillFilterRel filterRel = (DrillFilterRel) call.rel(0);
          final DrillProjectRel projectRel = (DrillProjectRel) call.rel(1);
          final DrillScanRel scanRel = (DrillScanRel) call.rel(2);
          doOnMatch(call, filterRel, projectRel, scanRel);
        }
      };

  public static final StoragePluginOptimizerRule HIVE_FILTER_ON_SCAN =
      new HivePushPartitionFilterIntoScan(
          RelOptHelper.some(DrillFilterRel.class, RelOptHelper.any(DrillScanRel.class)),
          "HivePushPartitionFilterIntoScan:Filter_On_Scan") {

        @Override
        public boolean matches(RelOptRuleCall call) {
          final DrillScanRel scan = (DrillScanRel) call.rel(1);
          GroupScan groupScan = scan.getGroupScan();
          return groupScan instanceof HiveScan &&  groupScan.supportsPartitionFilterPushdown();
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
          final DrillFilterRel filterRel = (DrillFilterRel) call.rel(0);
          final DrillScanRel scanRel = (DrillScanRel) call.rel(1);
          doOnMatch(call, filterRel, null, scanRel);
        }
      };

  private HivePushPartitionFilterIntoScan(
      RelOptRuleOperand operand,
      String id) {
    super(operand, id);
  }

  private HiveReadEntry splitFilter(HiveReadEntry origReadEntry, DirPathBuilder builder) {
    HiveTable table = origReadEntry.table;
    List<HivePartition> partitions = origReadEntry.partitions;
    List<HivePartition> newPartitions = new LinkedList<>();
    String pathPrefix = PartitionPruningUtil.truncatePrefixFromPath(table.getTable().getSd().getLocation());
    List<String> newFiles = Lists.newArrayList();
    List<String> dirPathList = builder.getDirPath();

    for (String dirPath : dirPathList) {
      String fullPath = pathPrefix + dirPath;
      // check containment of this path in the list of files
      for (HivePartition part: partitions) {
        String origFilePath = origReadEntry.getPartitionLocation(part);
        String origFileName = PartitionPruningUtil.truncatePrefixFromPath(origFilePath);

        if (origFileName.startsWith(fullPath)) {
          newFiles.add(origFileName);
          newPartitions.add(part);
        }
      }
    }

    if (newFiles.size() > 0) {
      HiveReadEntry newReadEntry = new HiveReadEntry(table, newPartitions, origReadEntry.hiveConfigOverride);
      return newReadEntry;
    }
    return origReadEntry;
  }

  protected void doOnMatch(RelOptRuleCall call, DrillFilterRel filterRel, DrillProjectRel projectRel, DrillScanRel scanRel) {
    DrillRel inputRel = projectRel != null ? projectRel : scanRel;
    HiveReadEntry origReadEntry = ((HiveScan)scanRel.getGroupScan()).hiveReadEntry;
    DirPathBuilder builder = new DirPathBuilder(filterRel, inputRel, filterRel.getCluster().getRexBuilder(), new HivePartitionDescriptor(origReadEntry.table.partitionKeys));
    HiveReadEntry newReadEntry = splitFilter(origReadEntry, builder);

    if (origReadEntry == newReadEntry) {
      return; // no directory filter was pushed down
    }

    try {
      HiveScan oldScan = (HiveScan) scanRel.getGroupScan();
      HiveScan hiveScan = new HiveScan(oldScan.getUserName(), newReadEntry, oldScan.storagePlugin, oldScan.columns);
      PartitionPruningUtil.rewritePlan(call, filterRel, projectRel, scanRel, hiveScan, builder);
    } catch (ExecutionSetupException e) {
      throw new DrillRuntimeException(e);
    }

  }
}
