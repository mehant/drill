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

  // TODO: Implement
  public int findMaxOutputRecordCount() {
    int maxRecords = rightCounts.get(nextRightBatchToProcess) - nextRightRecordToProcess;
    for (int i = nextRightBatchToProcess + 1; i < rightBatchCount; i++) {
      maxRecords += rightCounts.get(i);
    }

    maxRecords *= (leftRecordCount - nextLeftRecordToProcess);

    return maxRecords;
  }

  private int outputRecordsInternal() {
    outputRecords = 0;
    maxOutputRecords = findMaxOutputRecordCount();

    // TODO: Remove the check to see if we reached max record count from within the inner most loop. Simply calculate the limits once before the loop starts
    for (;nextRightBatchToProcess < rightBatchCount; nextRightBatchToProcess++) {
      int compositeIndexPart = nextRightBatchToProcess << 16;

      for (;nextRightRecordToProcess < rightCounts.get(nextRightBatchToProcess); nextRightRecordToProcess++) {
        for (;nextLeftRecordToProcess < leftRecordCount; nextLeftRecordToProcess++) {

          emitLeft(nextLeftRecordToProcess, outputIndex);
          emitRight((compositeIndexPart | (nextRightRecordToProcess & 0x0000FFFF)), outputIndex);
          outputIndex++;
          outputRecords++;

          if (outputRecords >= maxOutputRecords) {
            return outputRecords;
          }
        }
        nextLeftRecordToProcess = 0;
      }
      nextRightRecordToProcess = 0;
    }
    return outputRecords;
  }

  public int outputRecords() {
    int outputRecords = 0;
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
  /*
  public int outputRecords() {
    int outIndex = 0;

    if (drainLeft) {
      drainLeft = false;
      // no more records to process
      if (!getNextLeftBatch()) {
        return 0;
      }
    }

    // For every record in the left batch
    for (;nextLeftRecordToProcess < leftRecordCount; nextLeftRecordToProcess++) {
      // For every batch in the expandable right container
      for (int i = nextRightBatchToProcess; i < rightBatchCount; i++) {
        boolean done = false;
        // check if we have reached max number of output records
        int upperLimit = rightCounts.get(i);
        if (MAX_OUTPUT_RECORD - outIndex < upperLimit) {
          drainLeft = false;
          done = true;
          upperLimit = MAX_OUTPUT_RECORD - outIndex;
        }
        int compositeIndexPart = i << 16;
        int j;

        // For every record in this batch of the expandable right container
        for (j = nextRightRecordToProcess; j < upperLimit; j++) {
          emitLeft(nextLeftRecordToProcess, outIndex);
          emitRight((compositeIndexPart | (j & 0x0000FFFF)), outIndex);
          outIndex++;
        }

        if (done == true) {
          nextRightBatchToProcess = i;
          nextRightRecordToProcess = j;
          return outIndex;
        }
      }
    }

    // completely drained the left batch, need to invoke next
    drainLeft = true;

    // Clear the memory in the left record batch
    for (VectorWrapper<?> vw : left) {
      vw.getValueVector().clear();
    }
    return outIndex;
  }
  */

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
