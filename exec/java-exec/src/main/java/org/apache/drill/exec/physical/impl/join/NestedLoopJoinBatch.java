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

public class NestedLoopJoinBatch extends AbstractRecordBatch<NestedLoopJoinPOP> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NestedLoopJoinBatch.class);

  protected static final int MAX_BATCH_SIZE = 4096;

  protected static final int LEFT_INPUT = 0;
  protected static final int RIGHT_INPUT = 1;

  private RecordBatch left;
  private RecordBatch right;
  private NestedLoopJoin nljWorker = null;
  private BatchSchema leftSchema = null;
  private BatchSchema rightSchema = null;
  private int outputRecords = 0;
  private boolean getRight = true;
  private ExpandableHyperContainerContext containerContext = new ExpandableHyperContainerContext();

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
      IterOutcome rightUpstream;
      while (getRight == true) {
        rightUpstream = next(RIGHT_INPUT, right);
        switch (rightUpstream) {
          case OK_NEW_SCHEMA:
            if (!right.getSchema().equals(rightSchema)) {
              throw new DrillRuntimeException("Nested loop join does not handle schema change. Schema change" +
                  " found on the right side of NLJ.");
            }
            // fall through
          case OK:
            containerContext.addBatch(right);
           break;
          case NONE:
          case STOP:
          case NOT_YET:
            getRight = false;
            break;
        }
      }
      nljWorker.setupNestedLoopJoin(context, containerContext, left, this);
      state = BatchState.NOT_FIRST;
    }

    allocateVectors();

    outputRecords = nljWorker.outputRecords();

    // Set the record count
    for (VectorWrapper vw : container) {
      vw.getValueVector().getMutator().setValueCount(outputRecords);
    }
    container.setRecordCount(outputRecords);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);

    logger.debug("Number of records emitted: " + outputRecords);

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
      next(LEFT_INPUT, left);
      next(RIGHT_INPUT, right);

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

      containerContext.addBatch(right);

      nljWorker = setupWorker();

      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);

      allocateVectors();

      container.setRecordCount(0);

    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException(e);
    }
  }

  @Override
  public void cleanup() {
    containerContext.cleanup();
    super.cleanup();
    right.cleanup();
    left.cleanup();
  }

  private void allocateVectors() {
    for (VectorWrapper vw : container) {
      AllocationHelper.allocateNew(vw.getValueVector(), MAX_BATCH_SIZE);
    }
  }
}
