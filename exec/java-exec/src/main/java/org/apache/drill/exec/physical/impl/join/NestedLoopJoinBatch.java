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

import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JVar;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.NestedLoopJoinPOP;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.ExpandableHyperContainer;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;

import java.io.IOException;
import java.util.LinkedList;

public class NestedLoopJoinBatch extends AbstractRecordBatch<NestedLoopJoinPOP> {

  RecordBatch left;
  RecordBatch right;
  IterOutcome leftUpstream = IterOutcome.NONE;
  IterOutcome rightUpstream = IterOutcome.NONE;
  ExpandableHyperContainer rightContainer;
  LinkedList<Integer> rightRecordCounts = new LinkedList<>();
  NestedLoopJoin nljWorker = null;
  BatchSchema leftSchema = null;
  BatchSchema rightSchema = null;
  int outputRecords = 0;
  boolean getRight = true;

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NestedLoopJoinBatch.class);

  private static final GeneratorMapping EMIT_RIGHT =
      GeneratorMapping.create("doSetup"/* setup method */, "emitRight" /* eval method */, null /* reset */,
          null /* cleanup */);
  // Generator mapping for the build side : constant
  private static final GeneratorMapping EMIT_RIGHT_CONSTANT = GeneratorMapping.create("doSetup"/* setup method */,
      "doSetup" /* eval method */,
      null /* reset */, null /* cleanup */);

  // Generator mapping for the probe side : scalar
  private static final GeneratorMapping EMIT_LEFT =
      GeneratorMapping.create("doSetup" /* setup method */, "emitLeft" /* eval method */, null /* reset */,
          null /* cleanup */);
  // Generator mapping for the probe side : constant
  private static final GeneratorMapping EMIT_LEFT_CONSTANT = GeneratorMapping.create("doSetup" /* setup method */,
      "doSetup" /* eval method */,
      null /* reset */, null /* cleanup */);


  // Mapping set for the build side
  private final MappingSet emitRightMapping =
      new MappingSet("rightCompositeIndex" /* read index */, "outIndex" /* write index */, "rightContainer" /* read container */,
          "outgoing" /* write container */, EMIT_RIGHT_CONSTANT, EMIT_RIGHT);

  // Mapping set for the probe side
  private final MappingSet emitLeftMapping = new MappingSet("leftIndex" /* read index */, "outIndex" /* write index */,
      "leftBatch" /* read container */,
      "outgoing" /* write container */,
      EMIT_LEFT_CONSTANT, EMIT_LEFT);

  protected NestedLoopJoinBatch(NestedLoopJoinPOP popConfig, FragmentContext context, RecordBatch left, RecordBatch right) throws OutOfMemoryException {
    super(popConfig, context);
    this.left = left;
    this.right = right;
  }

  @Override
  public IterOutcome innerNext() {

    // Accumulate the record batch on the right in a hyper container
    if (state == BatchState.FIRST) {
      while (getRight == true) {
        rightUpstream = next(right);
        switch (rightUpstream) {
          case OK_NEW_SCHEMA:
            if (!right.getSchema().equals(rightSchema)) {
              throw new DrillRuntimeException("Nested loop join does not support schema changes");
            }
            // TODO: Do we need to do this?
          /*
          for (VectorWrapper vw : container) {
            //vw.getValueVector().allocateNew();
            // TODO replace this with constant from template file
            AllocationHelper.allocateNew(vw.getValueVector(), 4096);
          }
          */
          // fall through
          case OK:
            rightRecordCounts.addLast(right.getRecordCount());
            RecordBatchData nextBatch = new RecordBatchData(right);
            rightContainer.addBatch(nextBatch.getContainer());
            break;
          case NONE:
          case STOP:
          case NOT_YET:
            getRight = false;
            break;
          default:
            throw new DrillRuntimeException("Invalid state");
        }
      }
      nljWorker.setupNestedLoopJoin(context, rightContainer, rightRecordCounts, left, this);
      state = BatchState.NOT_FIRST;
    }

    // TODO: remove, figure out the right place
    for (VectorWrapper vw : container) {
      //vw.getValueVector().allocateNew();
      // TODO replace this with constant from template file
      AllocationHelper.allocateNew(vw.getValueVector(), 4096);
    }
    outputRecords = nljWorker.outputRecords();

    logger.debug("emitted " + outputRecords);

    // Set the record count
    for (VectorWrapper vw : container) {
      vw.getValueVector().getMutator().setValueCount(outputRecords);
    }
    container.setRecordCount(outputRecords);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);

    return (outputRecords > 0) ? IterOutcome.OK : IterOutcome.NONE;
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
  }

  @Override
  public int getRecordCount() {
    return outputRecords;
  }

  public NestedLoopJoin setupWorker() throws IOException, ClassTransformationException {
    final CodeGenerator<NestedLoopJoin> cg = CodeGenerator.get(NestedLoopJoin.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    ClassGenerator<NestedLoopJoin> g = cg.getRoot();


    g.setMappingSet(emitLeftMapping);
    JExpression outIndex = JExpr.direct("outIndex");
    JExpression leftIndex = JExpr.direct("leftIndex");


    int fieldId = 0;
    int outputFieldId = 0;
    for (VectorWrapper<?> vv : left) {
      TypeProtos.MajorType fieldType = vv.getField().getType();

      ValueVector v = container.addOrGet(MaterializedField.create(vv.getField().getPath(), fieldType));
      if (v instanceof AbstractContainerVector) {
        vv.getValueVector().makeTransferPair(v);
        v.clear();
      }

      JVar inVV = g.declareVectorValueSetupAndMember("leftBatch", new TypedFieldId(fieldType, false, fieldId));
      JVar outVV = g.declareVectorValueSetupAndMember("outgoing", new TypedFieldId(fieldType, false, outputFieldId));

      g.getEvalBlock().add(outVV.invoke("copyFromSafe").arg(leftIndex).arg(outIndex).arg(inVV));

      fieldId++;
      outputFieldId++;
    }

    fieldId = 0;
    g.setMappingSet(emitRightMapping);
    JExpression rightCompositeIndex = JExpr.direct("rightCompositeIndex");

    for (MaterializedField field : rightSchema) {

      TypeProtos.MajorType fieldType = field.getType();

      // make sure to project field with children for children to show up in the schema
      final MaterializedField projected = field.cloneWithType(fieldType);
      // Add the vector to our output container
      container.addOrGet(projected);

      JVar inVV = g.declareVectorValueSetupAndMember("rightContainer", new TypedFieldId(field.getType(), true, fieldId));
      JVar outVV = g.declareVectorValueSetupAndMember("outgoing", new TypedFieldId(fieldType, false, outputFieldId));
      g.getEvalBlock().add(outVV.invoke("copyFromSafe")
          .arg(rightCompositeIndex.band(JExpr.lit((int) Character.MAX_VALUE)))
          .arg(outIndex)
          .arg(inVV.component(rightCompositeIndex.shrz(JExpr.lit(16)))));

      fieldId++;
      outputFieldId++;
    }

    return context.getImplementationClass(cg);
  }

  @Override
  protected void buildSchema() throws SchemaChangeException {

    try {
      leftUpstream = next(left);
      rightUpstream = next(right);

      // TODO check for Iteroutcome.NONE in the incoming schema
      leftSchema = left.getSchema();
      rightSchema = right.getSchema();

      if (leftSchema != null) {
        for (VectorWrapper vw : left) {
          container.addOrGet(vw.getField());
        }
      }

      if (rightSchema != null) {
        for (VectorWrapper vw : right) {
          container.addOrGet(vw.getField());
        }
      }

      if (rightContainer == null) {
        rightRecordCounts.addLast(right.getRecordCount());
        RecordBatchData firstBatch = new RecordBatchData(right);
        rightContainer = new ExpandableHyperContainer(firstBatch.getContainer());
      }

      nljWorker = setupWorker();
      //nljWorker.setupNestedLoopJoin(context, rightContainer, rightRecordCounts, left, leftUpstream, this);

      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);

      // TODO do we need this?
      for (VectorWrapper vw : container) {
        //vw.getValueVector().allocateNew();
        AllocationHelper.allocateNew(vw.getValueVector(), 4096);
      }
      container.setRecordCount(0);

    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Cannot compile class");
    }
  }

  @Override
  public void cleanup() {
    if (rightContainer != null) {
      rightContainer.clear();
    }
    super.cleanup();
    right.cleanup();
    left.cleanup();
  }
}
