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
package org.apache.drill.exec.planner;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.partition.PruneScanRule;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.vector.ValueVector;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

/*
 * Interface that defines the behavior of different partitioning mechanism.
 * The implementations will be passed to PruneScanRule (logic that performs partition pruning)
 * to abstract away details of specific partitioning mechanism
 */
public interface PartitionPruningScheme {

  /**
   * Method creates an in memory representation of all the partitions. For each level of partitioning we
   * will create a value vector which this method will populate for all the partitions with the values of the
   * partioning key
   * @param vectors - Array of vectors in the container that need to be populated
   * @param partitions - List of all the partitions that exist in the table
   * @param partitionColumnBitSet - Partition columns selected in the query
   * @param fieldNameMap - Maps field ordinal to the field name
   * @param groupScan - Underlying group scan
   */
  void populatePartitionVectors(ValueVector[] vectors, List<PruneScanRule.PathPartition> partitions,
                                BitSet partitionColumnBitSet, Map<Integer, String> fieldNameMap, GroupScan groupScan);

  /**
   * Method returns the Major type associated with the given column
   * @param groupScan - Underlying scan operator
   * @param column - column whose type should be determined
   * @return
   */
  TypeProtos.MajorType getVectorType(GroupScan groupScan, SchemaPath column);

  /**
   * Get all the list of partitions associated with this scan
   * @param scanRel - scan operator
   * @return
   */
  List<String> getFiles(DrillScanRel scanRel);

  /**
   * Return a partition descriptor that defines the partition associated with the underlying scan operator
   * @param settings - Planner settings
   * @param scanRel - scan operator
   * @return
   */
  PartitionDescriptor getPartitionDescriptor(PlannerSettings settings, DrillScanRel scanRel);
}
