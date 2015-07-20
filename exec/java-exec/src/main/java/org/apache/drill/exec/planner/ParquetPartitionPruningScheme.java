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

import org.apache.calcite.util.BitSets;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.partition.PruneScanRule;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.store.ParquetOutputRecordWriter;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableDateVector;
import org.apache.drill.exec.vector.NullableDecimal18Vector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableSmallIntVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableTimeVector;
import org.apache.drill.exec.vector.NullableTinyIntVector;
import org.apache.drill.exec.vector.NullableUInt1Vector;
import org.apache.drill.exec.vector.NullableUInt2Vector;
import org.apache.drill.exec.vector.NullableUInt4Vector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeUtils;
import parquet.io.api.Binary;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Parquet based partition pruning scheme implementation.
 */
public class ParquetPartitionPruningScheme implements PartitionPruningScheme {

  /**
   * Go over the all the partitions and for every partition populate the corresponding
   * vector with the partitioning key.
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
        SchemaPath column = SchemaPath.getSimplePath(fieldNameMap.get(partitionColumnIndex));
        ((ParquetGroupScan) groupScan).populatePruningVector(vectors[partitionColumnIndex], record, column,
            partition.getFile());
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
   * Return the major type corresponding to the partition column
   * @param groupScan - Underlying scan operator
   * @param column - column whose type should be determined
   * @return
   */
  @Override
  public TypeProtos.MajorType getVectorType(GroupScan groupScan, SchemaPath column) {
    return ((ParquetGroupScan) groupScan).getTypeForColumn(column);
  }

  /**
   * Return list of all the partitioned files associated with this table
   * @param scanRel - scan operator
   * @return
   */
  @Override
  public List<String> getFiles(DrillScanRel scanRel) {
    ParquetGroupScan groupScan = (ParquetGroupScan) scanRel.getGroupScan();
    return new ArrayList(groupScan.getFileSet());
  }

  /**
   * Create and return Parquet based partioning descriptor
   * @param settings - Planner settings
   * @param scanRel - scan operator
   * @return
   */
  @Override
  public PartitionDescriptor getPartitionDescriptor(PlannerSettings settings, DrillScanRel scanRel) {
    return new ParquetPartitionDescriptor(settings, scanRel);
  }
}
