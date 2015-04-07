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

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;

import javax.inject.Named;
import java.util.List;

public abstract class NestedLoopJoinTemplate implements NestedLoopJoin {

  private RecordBatch left = null;
  private NestedLoopJoinBatch outgoing = null;
  private List<Integer> rightCounts = null;
  private int rightBatchCount = 0;
  private int leftRecordCount = 0;
  private int nextRightBatchToProcess = 0;
  private int nextRightRecordToProcess = 0;
  private int nextLeftRecordToProcess = 0;
  private int outputIndex = 0;

  public void setupNestedLoopJoin(FragmentContext context, ExpandableHyperContainerContext rightContainerContext,
                                  RecordBatch left, NestedLoopJoinBatch outgoing) {
    this.left = left;
    leftRecordCount = left.getRecordCount();
    this.rightBatchCount = rightContainerContext.getNumberOfBatches();
    this.rightCounts = rightContainerContext.getRecordCounts();
    this.outgoing = outgoing;

    doSetup(context, rightContainerContext.getContainer(), left, outgoing);
  }

  private boolean populateOutgoingBatch() {

    for (;nextRightBatchToProcess < rightBatchCount; nextRightBatchToProcess++) {
      int compositeIndexPart = nextRightBatchToProcess << 16;
      int rightRecordCount = rightCounts.get(nextRightBatchToProcess);

      for (;nextRightRecordToProcess < rightRecordCount; nextRightRecordToProcess++) {
        for (;nextLeftRecordToProcess < leftRecordCount; nextLeftRecordToProcess++) {

          emitLeft(nextLeftRecordToProcess, outputIndex);
          emitRight((compositeIndexPart | (nextRightRecordToProcess & 0x0000FFFF)), outputIndex);
          outputIndex++;

          // TODO: Optimization; We can eliminate this check and compute the limits before the loop
          if (outputIndex >= NestedLoopJoinBatch.MAX_BATCH_SIZE) {
            nextLeftRecordToProcess++;
            return false;
          }
        }
        nextLeftRecordToProcess = 0;
      }
      nextRightRecordToProcess = 0;
    }
    return true;
  }

  public int outputRecords() {
    outputIndex = 0;
    while (leftRecordCount != 0) {
      if (!populateOutgoingBatch()) {
        break;
      }
      // reset state and get next left batch
      resetAndGetNext();
    }
    return outputIndex;
  }

  private void resetAndGetNext() {

    for (VectorWrapper<?> vw : left) {
      vw.getValueVector().clear();
    }
    nextRightBatchToProcess = nextRightRecordToProcess = nextLeftRecordToProcess = 0;
    RecordBatch.IterOutcome leftOutcome = outgoing.next(NestedLoopJoinBatch.LEFT_INPUT, left);
    switch (leftOutcome) {
      case OK_NEW_SCHEMA:
        throw new DrillRuntimeException("Nested loop join does not handle schema change. Schema change" +
            " found on the left side of NLJ.");
      case NONE:
      case NOT_YET:
      case STOP:
        leftRecordCount = 0;
        break;
      case OK:
        leftRecordCount = left.getRecordCount();
        break;
    }
  }

  public abstract void doSetup(@Named("context") FragmentContext context,
                               @Named("rightContainer") VectorContainer rightContainer,
                               @Named("leftBatch") RecordBatch leftBatch,
                               @Named("outgoing") RecordBatch outgoing);

  public abstract void emitRight(@Named("rightCompositeIndex") int rightCompositeIndex,
                                 @Named("outIndex") int outIndex);

  public abstract void emitLeft(@Named("leftIndex") int leftIndex, @Named("outIndex") int outIndex);
}
