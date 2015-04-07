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
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;

import java.io.IOException;

/*
 * RecordBatch implementation for the nested loop join operator
 */
public class NestedLoopJoinBatch extends AbstractRecordBatch<NestedLoopJoinPOP> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NestedLoopJoinBatch.class);

  // Maximum number records in the outgoing batch
  protected static final int MAX_BATCH_SIZE = 4096;

  // Input indexes to correctly update the stats
  protected static final int LEFT_INPUT = 0;
  protected static final int RIGHT_INPUT = 1;

  // Left input to the nested loop join operator
  private RecordBatch left;

  // Right input to the nested loop join operator.
  private RecordBatch right;

  // Runtime generated class implementing the NestedLoopJoin interface
  private NestedLoopJoin nljWorker = null;

  // Schema on the left side
  private BatchSchema leftSchema = null;

  // Schema on the right side
  private BatchSchema rightSchema = null;

  // Number of output records in the current outgoing batch
  private int outputRecords = 0;

  /*
   * We accumulate all the batches on the right side in a hyper container. The below
   * context keeps track of the hyper container and also maintains a list of record counts
   * per input batch
   */
  private ExpandableHyperContainerContext containerContext = new ExpandableHyperContainerContext();

  // Generator mapping for the right side
  private static final GeneratorMapping EMIT_RIGHT =
      GeneratorMapping.create("doSetup"/* setup method */, "emitRight" /* eval method */, null /* reset */,
          null /* cleanup */);
  // Generator mapping for the right side : constant
  private static final GeneratorMapping EMIT_RIGHT_CONSTANT = GeneratorMapping.create("doSetup"/* setup method */,
      "doSetup" /* eval method */,
      null /* reset */, null /* cleanup */);

  // Generator mapping for the left side : scalar
  private static final GeneratorMapping EMIT_LEFT =
      GeneratorMapping.create("doSetup" /* setup method */, "emitLeft" /* eval method */, null /* reset */,
          null /* cleanup */);
  // Generator mapping for the left side : constant
  private static final GeneratorMapping EMIT_LEFT_CONSTANT = GeneratorMapping.create("doSetup" /* setup method */,
      "doSetup" /* eval method */,
      null /* reset */, null /* cleanup */);


  // Mapping set for the right side
  private final MappingSet emitRightMapping =
      new MappingSet("rightCompositeIndex" /* read index */, "outIndex" /* write index */, "rightContainer" /* read container */,
          "outgoing" /* write container */, EMIT_RIGHT_CONSTANT, EMIT_RIGHT);

  // Mapping set for the left side
  private final MappingSet emitLeftMapping = new MappingSet("leftIndex" /* read index */, "outIndex" /* write index */,
      "leftBatch" /* read container */,
      "outgoing" /* write container */,
      EMIT_LEFT_CONSTANT, EMIT_LEFT);

  protected NestedLoopJoinBatch(NestedLoopJoinPOP popConfig, FragmentContext context, RecordBatch left, RecordBatch right) throws OutOfMemoryException {
    super(popConfig, context);
    this.left = left;
    this.right = right;
  }

  /**
   * Method drains the right side input of the NLJ and accumulates the data
   * in a hyper container. Once we have all the data from the right side we
   * process the left side one batch at a time and produce the output batch
   * which is a cross product of the two sides.
   * @return IterOutcome state of the nested loop join batch
   */
  @Override
  public IterOutcome innerNext() {

    // Accumulate batches on the right in a hyper container
    if (state == BatchState.FIRST) {
      IterOutcome rightUpstream;
      boolean drainRight = true;
      while (drainRight == true) {
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
            drainRight = false;
            break;
        }
      }
      nljWorker.setupNestedLoopJoin(context, left, containerContext, this);
      state = BatchState.NOT_FIRST;
    }

    // allocate space for the outgoing batch
    allocateVectors();

    // invoke the runtime generated method to emit records in the output batch
    outputRecords = nljWorker.outputRecords();

    // Set the record count
    for (VectorWrapper vw : container) {
      vw.getValueVector().getMutator().setValueCount(outputRecords);
    }

    // Set the record count in the container
    container.setRecordCount(outputRecords);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);

    logger.debug("Number of records emitted: " + outputRecords);

    return (outputRecords > 0) ? IterOutcome.OK : IterOutcome.NONE;
  }

  /**
   * Method generates the runtime code needed for NLJ. Other than the setup method to set the input and output value
   * vector references we implement two more methods
   * 1. emitLeft()  -> Project record from the left side
   * 2. emitRight() -> Project record from the right side (which is a hyper container)
   * @return the runtime generated class that implements the NestedLoopJoin interface
   * @throws IOException
   * @throws ClassTransformationException
   */
  private NestedLoopJoin setupWorker() throws IOException, ClassTransformationException {
    final CodeGenerator<NestedLoopJoin> cg = CodeGenerator.get(NestedLoopJoin.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    ClassGenerator<NestedLoopJoin> g = cg.getRoot();


    g.setMappingSet(emitLeftMapping);
    JExpression outIndex = JExpr.direct("outIndex");
    JExpression leftIndex = JExpr.direct("leftIndex");


    int fieldId = 0;
    int outputFieldId = 0;
    // Set the input and output value vector references corresponding to the left batch
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

    // Set the input and output value vector references corresponding to the left batch
    for (MaterializedField field : rightSchema) {

      TypeProtos.MajorType fieldType = field.getType();
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

  /**
   * Simple method to allocate space for all the vectors in the container.
   */
  private void allocateVectors() {
    for (VectorWrapper vw : container) {
      AllocationHelper.allocateNew(vw.getValueVector(), MAX_BATCH_SIZE);
    }
  }

  /**
   * Builds the output container's schema. Goes over the left and the right
   * batch and adds the corresponding vectors to the output container.
   * @throws SchemaChangeException
   */
  @Override
  protected void buildSchema() throws SchemaChangeException {

    try {
      next(LEFT_INPUT, left);
      next(RIGHT_INPUT, right);

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

  @Override
  protected void killIncoming(boolean sendUpstream) {
    this.left.kill(sendUpstream);
    this.right.kill(sendUpstream);
  }

  @Override
  public int getRecordCount() {
    return outputRecords;
  }
}
