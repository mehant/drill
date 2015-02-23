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

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;

import javax.inject.Named;

public abstract class NestedLoopJoinTemplate implements NestedLoopJoin {

  FragmentContext context = null;
  VectorContainer right = null;
  RecordBatch left = null;
  RecordBatch outgoing = null;

  public void setupNestedLoopJoin(FragmentContext context, VectorContainer right, RecordBatch left, NestedLoopJoinBatch outgoing) {
    this.context = context;
    this.right = right;
    this.left = left;
    this.outgoing = outgoing;

    doSetup(context, right, left, outgoing);
  }

  public int outputRecords() {
    emitLeft(0, 0);
    emitRight(0, 0);
    return 1;
  }

  public abstract void doSetup(@Named("context") FragmentContext context, @Named("rightContainer") VectorContainer rightContainer, @Named("leftBatch") RecordBatch leftBatch,
                               @Named("outgoing") RecordBatch outgoing);
  public abstract void emitRight(@Named("rightCompositeIndex") int rightCompositeIndex, @Named("outIndex") int outIndex);
  public abstract void emitLeft(@Named("leftIndex") int leftIndex, @Named("outIndex") int outIndex);
}
