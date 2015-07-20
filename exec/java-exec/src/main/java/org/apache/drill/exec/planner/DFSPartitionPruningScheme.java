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

import com.google.common.base.Charsets;
import org.apache.calcite.util.BitSets;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.partition.PruneScanRule;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;

import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * DFS based partition pruning scheme implementation.
 */
public class DFSPartitionPruningScheme implements PartitionPruningScheme {

  /**
   * Go over the all the partition files and populate all the vectors with the
   * corresponding values.
   * @param vectors - Array of vectors in the container that need to be populated
   * @param partitions - List of all the partitions that exist in the table
   * @param partitionColumnBitSet - Partition columns selected in the query
   * @param fieldNameMap - Maps field ordinal to the field name
   * @param groupScan - Underlying group scan
   */
  @Override
  public void populatePartitionVectors(ValueVector[] vectors, List<PruneScanRule.PathPartition> partitions,
                                       BitSet partitionColumnBitSet, Map<Integer, String> fieldNameMap,
                                       GroupScan groupScan) {

    int record = 0;
    for (Iterator<PruneScanRule.PathPartition> iter = partitions.iterator(); iter.hasNext(); record++) {
      final PruneScanRule.PathPartition partition = iter.next();
      for (int partitionColumnIndex : BitSets.toIter(partitionColumnBitSet)) {
        if (partition.getDirs()[partitionColumnIndex] == null) {
          ((NullableVarCharVector) vectors[partitionColumnIndex]).getMutator().setNull(record);
        } else {
          byte[] bytes = partition.getDirs()[partitionColumnIndex].getBytes(Charsets.UTF_8);
          ((NullableVarCharVector) vectors[partitionColumnIndex]).getMutator().setSafe(record, bytes, 0, bytes.length);
        }
      }
    }

    for (ValueVector v : vectors) {
      if (v == null) {
        continue;
      }
      v.getMutator().setValueCount(partitions.size());
    }

  }

  /**
   * Currently all the partition keys for DFS based partitions are treated as VARCHAR/
   * @param groupScan - Underlying scan operator
   * @param column - column whose type should be determined
   * @return - VARCHAR major type
   */
  @Override
  public TypeProtos.MajorType getVectorType(GroupScan groupScan, SchemaPath column) {
    return Types.optional(TypeProtos.MinorType.VARCHAR);
  }

  /**
   * Return all the partition files for this table
   * @param scanRel - scan operator
   * @return
   */
  @Override
  public List<String> getFiles(DrillScanRel scanRel) {
    return ((FormatSelection) scanRel.getDrillTable().getSelection()).getAsFiles();
  }

  /**
   * Create and return the DFS based partition descriptor
   * @param settings - Planner settings
   * @param scanRel - scan operator
   * @return
   */
  @Override
  public PartitionDescriptor getPartitionDescriptor(PlannerSettings settings, DrillScanRel scanRel) {
    return new FileSystemPartitionDescriptor(settings.getFsPartitionColumnLabel());
  }
}
