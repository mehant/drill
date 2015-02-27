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

package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.Decimal38Holder;
import org.apache.drill.exec.record.RecordBatch;

import javax.inject.Inject;
import java.math.BigDecimal;
import java.math.BigInteger;


public class Decimal38Functions {
  @FunctionTemplate(name = "add", scope = FunctionTemplate.FunctionScope.DECIMAL_ADD_SCALE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Decimal38Decimal38Add implements DrillSimpleFunc {

    @Param  Decimal38Holder input1;
    @Param  Decimal38Holder input2;
    @Inject DrillBuf buffer;
    @Output Decimal38Holder output;

    @Override
    public void setup(RecordBatch incoming) {
      buffer = buffer.reallocIfNeeded(Decimal38Holder.WIDTH);
    }

    @Override
    public void eval() {
      output.start = 0;
      output.buffer = buffer;
      org.apache.drill.exec.expr.fn.impl.Decimal38FunctionImplementation.addDecimal38Decimal38(input1.buffer, input1.start, input1.scale, input1.precision,
          input2.buffer, input2.start, input2.scale, input2.precision, output.buffer, output.start, 2, 38);
    }
  }
}
