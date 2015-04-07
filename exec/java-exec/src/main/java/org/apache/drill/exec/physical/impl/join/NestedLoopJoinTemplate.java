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

/*
 * Template class that combined with the runtime generated source implements the NestedLoopJoin interface. This
 * class contains the main nested loop join logic.
 */
public abstract class NestedLoopJoinTemplate implements NestedLoopJoin {

  // Current left input batch being processed
  private RecordBatch left = null;

  // Record count of the left batch currently being processed
  private int leftRecordCount = 0;

  // List of record counts  per batch in the hyper container
  private List<Integer> rightCounts = null;

  // Total number of batches on the right side
  private int totalRightBatches = 0;

  // Output batch
  private NestedLoopJoinBatch outgoing = null;

  // Current index to be populated in the output batch
  private int outputIndex = 0;

  // Next right batch to process
  private int nextRightBatchToProcess = 0;

  // Next record in the current right batch to process
  private int nextRightRecordToProcess = 0;

  // Next record in the left batch to process
  private int nextLeftRecordToProcess = 0;

  /**
   * Method initializes necessary state and invokes the doSetup() to set the
   * input and output value vector references
   * @param context Fragment context
   * @param left Current left input batch being processed
   * @param rightContainerContext hyper container context
   * @param outgoing output batch
   */
  public void setupNestedLoopJoin(FragmentContext context, RecordBatch left,
                                  ExpandableHyperContainerContext rightContainerContext,
                                  NestedLoopJoinBatch outgoing) {
    this.left = left;
    leftRecordCount = left.getRecordCount();
    this.totalRightBatches = rightContainerContext.getNumberOfBatches();
    this.rightCounts = rightContainerContext.getRecordCounts();
    this.outgoing = outgoing;

    doSetup(context, rightContainerContext.getContainer(), left, outgoing);
  }

  /**
   * This method is the core of the nested loop join. For every record on the right we go over
   * the left batch and produce the cross product output
   * @return a boolean indicating if there is more space in the output batch and can continue processing more left batches
   */
  private boolean populateOutgoingBatch() {

    for (;nextRightBatchToProcess < totalRightBatches; nextRightBatchToProcess++) { // for every batch on the right
      int compositeIndexPart = nextRightBatchToProcess << 16;
      int rightRecordCount = rightCounts.get(nextRightBatchToProcess);

      for (;nextRightRecordToProcess < rightRecordCount; nextRightRecordToProcess++) { // for every record in this right batch
        for (;nextLeftRecordToProcess < leftRecordCount; nextLeftRecordToProcess++) { // for every record in the left batch

          // project records from the left and right batches
          emitLeft(nextLeftRecordToProcess, outputIndex);
          emitRight((compositeIndexPart | (nextRightRecordToProcess & 0x0000FFFF)), outputIndex);
          outputIndex++;

          // TODO: Optimization; We can eliminate this check and compute the limits before the loop
          if (outputIndex >= NestedLoopJoinBatch.MAX_BATCH_SIZE) {
            nextLeftRecordToProcess++;

            // no more space left in the batch, stop processing
            return false;
          }
        }
        nextLeftRecordToProcess = 0;
      }
      nextRightRecordToProcess = 0;
    }

    // done with the current left batch and there is space in the output batch continue processing
    return true;
  }

  /**
   * Main entry point for producing the output records. Thin wrapper around populateOutgoingBatch(), this method
   * controls which left batch we are processing and fetches the next left input batch one we exhaust
   * the current one.
   * @return the number of records produced in the output batch
   */
  public int outputRecords() {
    outputIndex = 0;
    while (leftRecordCount != 0) {
      if (!populateOutgoingBatch()) {
        break;
      }
      // reset state and get next left batch
      resetAndGetNextLeft();
    }
    return outputIndex;
  }

  /**
   * Utility method to clear the memory in the left input batch once we have completed processing it. Resets some
   * internal state which indicate the next records to process in the left and right batches. Also fetches the next
   * left input batch.
   */
  private void resetAndGetNextLeft() {

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
