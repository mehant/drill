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

import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.record.ExpandableHyperContainer;
import org.apache.drill.exec.record.RecordBatch;

import java.util.LinkedList;
import java.util.List;

/**
 * A thin wrapper around the ExpandableHyperContainer. In addition to the hyper container
 * we also maintain a list of record counts corresponding to each batch in the hyper container.
 */
public class ExpandableHyperContainerContext {
  private ExpandableHyperContainer container = null;
  private LinkedList<Integer> recordCounts = new LinkedList<>();

  public ExpandableHyperContainer getContainer() {
    return container;
  }

  public List<Integer> getRecordCounts() {
    return recordCounts;
  }

  public void addBatch(RecordBatch batch) {
    RecordBatchData batchCopy = new RecordBatchData(batch);
    recordCounts.addLast(batch.getRecordCount());
    if (container == null) {
      container = new ExpandableHyperContainer(batchCopy.getContainer());
    } else {
      container.addBatch(batchCopy.getContainer());
    }
  }

  public int getNumberOfBatches() {
    return recordCounts.size();
  }

  public void cleanup() {
    if (container != null) {
      container.clear();
    }
    recordCounts = null;
  }
}
