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
package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.NestedLoopJoinPOP;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.NullableBigIntVector;

public class NestedLoopJoinBatch extends AbstractRecordBatch<NestedLoopJoinPOP> {

  RecordBatch left;
  RecordBatch right;
  IterOutcome outcome = IterOutcome.OK_NEW_SCHEMA;
  NullableBigIntVector vector;

  protected NestedLoopJoinBatch(NestedLoopJoinPOP popConfig, FragmentContext context, RecordBatch left, RecordBatch right) throws OutOfMemoryException {
    super(popConfig, context);
    this.left = left;
    this.right = right;
  }

  @Override
  public IterOutcome innerNext() {
    if (outcome == IterOutcome.OK_NEW_SCHEMA) {
      // Simply exhaust the two sides
      while (left.next() != IterOutcome.NONE) {
      }
      while (right.next() != IterOutcome.NONE){
      }

      // Set the schema for the output container
      vector = container.addOrGet("foo", TypeProtos.MajorType.newBuilder().setMode(TypeProtos.DataMode.OPTIONAL).setMinorType(TypeProtos.MinorType.BIGINT).build(), NullableBigIntVector.class);
      vector.allocateNew(1);
      container.setRecordCount(1);
      vector.getMutator().set(0, 100);
      vector.getMutator().setValueCount(1);
      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      outcome = IterOutcome.NONE;
      return IterOutcome.OK;
    }
    return outcome;
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
  }

  @Override
  public int getRecordCount() {
    return 1;
  }

  @Override
  protected void buildSchema() throws SchemaChangeException {
    vector = container.addOrGet("foo", TypeProtos.MajorType.newBuilder().setMode(TypeProtos.DataMode.OPTIONAL).setMinorType(TypeProtos.MinorType.BIGINT).build(), NullableBigIntVector.class);
    vector.allocateNew();
    vector.getMutator().setValueCount(0);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    container.setRecordCount(0);
  }
}
