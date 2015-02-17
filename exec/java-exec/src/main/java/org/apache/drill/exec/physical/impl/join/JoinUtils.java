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
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.resolver.TypeCastRules;

import java.util.LinkedList;
import java.util.List;

public class JoinUtils {
  public static enum JoinComparator {
    NONE, // No comparator
    EQUALS, // Equality comparator
    IS_NOT_DISTINCT_FROM // 'IS NOT DISTINCT FROM' comparator
  }

  // Check the comparator for the join condition. Note that a similar check is also
  // done in JoinPrel; however we have to repeat it here because a physical plan
  // may be submitted directly to Drill.
  public static JoinComparator checkAndSetComparison(JoinCondition condition,
                                                     JoinComparator comparator) {
    if (condition.getRelationship().equalsIgnoreCase("EQUALS") ||
        condition.getRelationship().equals("==") /* older json plans still have '==' */) {
      if (comparator == JoinComparator.NONE ||
          comparator == JoinComparator.EQUALS) {
        return JoinComparator.EQUALS;
      } else {
        throw new IllegalArgumentException("This type of join does not support mixed comparators.");
      }
    } else if (condition.getRelationship().equalsIgnoreCase("IS_NOT_DISTINCT_FROM")) {
      if (comparator == JoinComparator.NONE ||
          comparator == JoinComparator.IS_NOT_DISTINCT_FROM) {
        return JoinComparator.IS_NOT_DISTINCT_FROM;
      } else {
        throw new IllegalArgumentException("This type of join does not support mixed comparators.");
      }
    }
    throw new IllegalArgumentException("Invalid comparator supplied to this join.");
  }


  public static void addLeastRestrictiveCasts(LogicalExpression[] keyExprsBuild, RecordBatch incomingBuild,
                                              LogicalExpression[] keyExprsProbe, RecordBatch incomingProbe,
                                              FragmentContext context) {

    // If we don't have probe expressions then nothing to do get out
    if (keyExprsProbe == null) {
      return;
    }

    assert keyExprsBuild.length == keyExprsProbe.length;

    for (int i = 0; i < keyExprsBuild.length; i++) {
      LogicalExpression buildExpr = keyExprsBuild[i];
      LogicalExpression probeExpr = keyExprsProbe[i];
      TypeProtos.MinorType buildType = buildExpr.getMajorType().getMinorType();
      TypeProtos.MinorType probeType = probeExpr.getMajorType().getMinorType();

      if (buildType != probeType) {
        // We need to add a cast to one of the expressions
        List<TypeProtos.MinorType> types = new LinkedList<>();
        types.add(buildType);
        types.add(probeType);
        TypeProtos.MinorType result = TypeCastRules.getLeastRestrictiveType(types);
        ErrorCollector errorCollector = new ErrorCollectorImpl();

        if (result == null) {
          throw new DrillRuntimeException(String.format("Join conditions cannot be compared failing build " +
                  "expression:" + " %s failing probe expression: %s", buildExpr.getMajorType().toString(),
              probeExpr.getMajorType().toString()));
        } else if (result != buildType) {
          // Add a cast expression on top of the build expression
          LogicalExpression castExpr = ExpressionTreeMaterializer.addCastExpression(buildExpr, probeExpr.getMajorType(), context.getFunctionRegistry(), errorCollector);
          // Store the newly casted expression
          keyExprsBuild[i] =
              ExpressionTreeMaterializer.materialize(castExpr, incomingBuild, errorCollector,
                  context.getFunctionRegistry());
        } else if (result != probeType) {
          // Add a cast expression on top of the probe expression
          LogicalExpression castExpr = ExpressionTreeMaterializer.addCastExpression(probeExpr, buildExpr.getMajorType(), context.getFunctionRegistry(), errorCollector);
          // store the newly casted expression
          keyExprsProbe[i] =
              ExpressionTreeMaterializer.materialize(castExpr, incomingProbe, errorCollector,
                  context.getFunctionRegistry());
        }
      }
    }
  }
}
