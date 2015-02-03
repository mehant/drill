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
package org.apache.drill.common.expression;

import com.google.common.collect.Iterators;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.CoreDecimalUtility;

import java.math.BigDecimal;
import java.util.Iterator;

// Base class used for all constant decimal expressions
public abstract class DecimalExpressionBase extends LogicalExpressionBase {

  BigDecimal value;

  protected DecimalExpressionBase(BigDecimal value, ExpressionPosition pos) {
    super(pos);
    this.value = value;
  }

 @Override
  public TypeProtos.MajorType getMajorType() {
    int precision = value.precision();
    return TypeProtos.MajorType.newBuilder().setMinorType(CoreDecimalUtility.getDecimalDataType(precision)).
        setScale(value.scale()).setPrecision(precision).setMode(TypeProtos.DataMode.REQUIRED).build();
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return Iterators.emptyIterator();
  }

  @Override
  public int getSelfCost() {
    return 0;
  }

  @Override
  public int getCumulativeCost() {
    return 0;
  }

  public int getScale() {
    return value.scale();
  }

  public int getPrecision() {
    return value.precision();
  }

  public BigDecimal getValue() {
    return value;
  }
}

