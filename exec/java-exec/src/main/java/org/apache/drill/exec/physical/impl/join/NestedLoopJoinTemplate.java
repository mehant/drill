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

  FragmentContext context = null;
  VectorContainer right = null;
  RecordBatch left = null;
  RecordBatch outgoing = null;
  List<Integer> rightCounts = null;
  int rightBatchCount = 0;
  int leftRecordCount = 0;
  boolean drainLeft = false;
  private static final int MAX_OUTPUT_RECORD = 4096;
  int nextRightBatchToProcess = 0;
  int nextRightRecordToProcess = 0;
  int nextLeftRecordToProcess = 0;
  int outputIndex = 0;
  // TODO: Make the left iteroutcome variable name same in nestedloopjoinbatch and in this template
  RecordBatch.IterOutcome leftState = RecordBatch.IterOutcome.NONE;

  // TODO REMOVE, added for debugging
  int maxOutputRecords = 0;
  int outputRecords = 0;

  public void setupNestedLoopJoin(FragmentContext context, VectorContainer right, List<Integer> rightCounts,
                                  RecordBatch left, RecordBatch.IterOutcome leftState, NestedLoopJoinBatch outgoing) {
    this.context = context;
    this.right = right;
    this.left = left;
    leftRecordCount = left.getRecordCount();
    if (leftRecordCount == 0) {
      drainLeft = true;
    }
    this.outgoing = outgoing;
    this.rightBatchCount = rightCounts.size();
    this.rightCounts = rightCounts;
    this.leftState = leftState;

    doSetup(context, right, left, outgoing);
  }

  // TODO: More readable, will be used when max limits per output iteration is calculated once and condition is not checked every time
  public int findMaxOutputRecordCount() {
    int pendingRecords = (leftRecordCount - nextLeftRecordToProcess);
    pendingRecords += ((rightCounts.get(nextRightBatchToProcess) - (nextRightRecordToProcess + 1)) * leftRecordCount);
    int maxRecords = 0;
    //int maxRecords = rightCounts.get(nextRightBatchToProcess) - nextRightRecordToProcess;
    for (int i = nextRightBatchToProcess + 1; i < rightBatchCount; i++) {
      maxRecords += rightCounts.get(i);
    }

    maxRecords *= (leftRecordCount);
    maxRecords += pendingRecords;

    NestedLoopJoinBatch.methodBreakPoint(nextRightBatchToProcess, nextRightRecordToProcess, nextLeftRecordToProcess, maxRecords);
    return Math.min(maxRecords, MAX_OUTPUT_RECORD);
  }

  private int outputRecordsInternal() {
    int outputRecords = 0;
    //maxOutputRecords = findMaxOutputRecordCount();

    for (;nextRightBatchToProcess < rightBatchCount; nextRightBatchToProcess++) {
      int compositeIndexPart = nextRightBatchToProcess << 16;
      int rightRecordCount = rightCounts.get(nextRightBatchToProcess);

      for (;nextRightRecordToProcess < rightRecordCount; nextRightRecordToProcess++) {
        for (;nextLeftRecordToProcess < leftRecordCount; nextLeftRecordToProcess++) {

          emitLeft(nextLeftRecordToProcess, outputIndex);
          emitRight((compositeIndexPart | (nextRightRecordToProcess & 0x0000FFFF)), outputIndex);
          outputIndex++;
          outputRecords++;

          // TODO: Remove the check to see if we reached max record count from within the inner most loop. Simply calculate the limits once before the loop starts
          if ((this.outputRecords + outputRecords)>= MAX_OUTPUT_RECORD) {
            NestedLoopJoinBatch.methodBreakPoint(nextRightBatchToProcess, nextRightRecordToProcess, nextLeftRecordToProcess, outputRecords);
            nextLeftRecordToProcess++;
            return outputRecords;
          }
        }
        nextLeftRecordToProcess = 0;
      }
      nextRightRecordToProcess = 0;
    }
    NestedLoopJoinBatch.methodBreakPoint(nextRightBatchToProcess, nextRightRecordToProcess, nextLeftRecordToProcess, outputRecords);
    return outputRecords;
  }

  public int outputRecords() {
    outputRecords = 0;
    while (leftState != RecordBatch.IterOutcome.NONE) {
      outputRecords += outputRecordsInternal();

      if (outputRecords == MAX_OUTPUT_RECORD) {
        outputIndex = 0;
        return outputRecords;
      }

      // reset state and get next left batch
      resetAndGetNext();
    }
    return outputRecords;
  }

  private void resetAndGetNext() {

    for (VectorWrapper<?> vw : left) {
      vw.getValueVector().clear();
    }
    nextRightBatchToProcess = nextRightRecordToProcess = nextLeftRecordToProcess = 0;
    RecordBatch.IterOutcome leftOutcome = left.next();
    switch (leftOutcome) {
      case OK_NEW_SCHEMA:
        throw new DrillRuntimeException("Nested loop join does not handle schema changes");
      case NONE:
      case NOT_YET:
      case STOP:
        // TODO: Handle this more gracefully
        leftState = RecordBatch.IterOutcome.NONE;
        break;
      case OK:
        leftRecordCount = left.getRecordCount();
        break;
      default:
        throw new UnsupportedOperationException("Incorrect state in nested loop join");
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
