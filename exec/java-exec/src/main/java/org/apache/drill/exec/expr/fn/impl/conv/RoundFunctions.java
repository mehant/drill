/*******************************************************************************

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
 ******************************************************************************/
package org.apache.drill.exec.expr.fn.impl.conv;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.SmallIntHolder;
import org.apache.drill.exec.expr.holders.TinyIntHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.UInt1Holder;
import org.apache.drill.exec.expr.holders.UInt2Holder;
import org.apache.drill.exec.expr.holders.UInt4Holder;
import org.apache.drill.exec.expr.holders.UInt8Holder;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;


public class RoundFunctions {

  /*
   * Following are round functions with no parameter. Per the SQL standard we simply return the same output
   * type as the input type for exact inputs (int, bigint etc) and inexact types (float, double).
   */
  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class RoundInt implements DrillSimpleFunc {

    @Param  IntHolder in;
    @Output IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }

  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundBigInt implements DrillSimpleFunc {

    @Param BigIntHolder in;
    @Output BigIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }

  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundFloat4 implements DrillSimpleFunc {

    @Param Float4Holder in;
    @Output Float4Holder out;

    public void setup() {
    }

    public void eval() {
      java.math.BigDecimal input = java.math.BigDecimal.valueOf(in.value);
      out.value = input.setScale(0, java.math.RoundingMode.HALF_UP).floatValue();
    }
  }

  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundFloat8 implements DrillSimpleFunc {

    @Param Float8Holder in;
    @Output Float8Holder out;

    public void setup() {
    }

    public void eval() {
      java.math.BigDecimal input = java.math.BigDecimal.valueOf(in.value);
      out.value = input.setScale(0, java.math.RoundingMode.HALF_UP).doubleValue();
    }
  }


  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundSmallInt implements DrillSimpleFunc {

    @Param SmallIntHolder in;
    @Output SmallIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }


  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundTinyInt implements DrillSimpleFunc {

    @Param TinyIntHolder in;
    @Output TinyIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }


  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundUInt1 implements DrillSimpleFunc {

    @Param UInt1Holder in;
    @Output UInt1Holder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }


  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundUInt2 implements DrillSimpleFunc {

    @Param UInt2Holder in;
    @Output UInt2Holder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }


  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundUInt4 implements DrillSimpleFunc {

    @Param UInt4Holder in;
    @Output UInt4Holder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }


  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundUInt8 implements DrillSimpleFunc {

    @Param UInt8Holder in;
    @Output UInt8Holder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }

  /*
   * Following are round functions which accept a parameter indicating the number of digits to be
   * maintained after the decimal point.
   * The input and output will be the same for integer types like int, bigint, smallint etc.
   * However for float and double we need to calculate the rounded value.
   */
  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundWithParamInt implements DrillSimpleFunc {

    @Param IntHolder in;
    @Param IntHolder scale;
    @Output IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }

  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundWithParamBigInt implements DrillSimpleFunc {

    @Param BigIntHolder in;
    @Param IntHolder scale;
    @Output BigIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }

  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundWithParamFloat4 implements DrillSimpleFunc {

    @Param Float4Holder in;
    @Param IntHolder scale;
    @Output Float4Holder out;

    public void setup() {
    }

    public void eval() {
      java.math.BigDecimal input = new java.math.BigDecimal(in.value);
      out.value = input.setScale(scale.value, java.math.RoundingMode.HALF_UP).floatValue();
    }
  }

  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundWithParamFloat8 implements DrillSimpleFunc {

    @Param Float8Holder in;
    @Param IntHolder scale;
    @Output Float8Holder out;

    public void setup() {
    }

    public void eval() {
      java.math.BigDecimal input = new java.math.BigDecimal(in.value);
      out.value = input.setScale(scale.value, java.math.RoundingMode.HALF_UP).doubleValue();
    }
  }

  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundWithParamSmallInt implements DrillSimpleFunc {

    @Param SmallIntHolder in;
    @Param IntHolder scale;
    @Output SmallIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }

  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundWithParamTinyInt implements DrillSimpleFunc {

    @Param TinyIntHolder in;
    @Param IntHolder scale;
    @Output TinyIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }

  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundWithParamUInt1 implements DrillSimpleFunc {

    @Param UInt1Holder in;
    @Param IntHolder scale;
    @Output UInt1Holder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }

  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundWithParamUInt2 implements DrillSimpleFunc {

    @Param UInt2Holder in;
    @Param IntHolder scale;
    @Output UInt2Holder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }

  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundWithParamUInt4 implements DrillSimpleFunc {

    @Param UInt4Holder in;
    @Param IntHolder scale;
    @Output UInt4Holder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }

  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundWithParamUInt8 implements DrillSimpleFunc {

    @Param UInt8Holder in;
    @Param IntHolder scale;
    @Output UInt8Holder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }
}
