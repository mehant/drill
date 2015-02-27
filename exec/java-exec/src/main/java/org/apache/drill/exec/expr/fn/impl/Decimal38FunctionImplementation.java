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
package org.apache.drill.exec.expr.fn.impl;


import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.expr.holders.Decimal38Holder;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.Decimal38Vector;

import java.math.BigDecimal;

public class Decimal38FunctionImplementation {

  private static final int WIDTH = Decimal38Holder.WIDTH;

  public static void addDecimal38Decimal38(DrillBuf buf1, int start1, int scale1, int precision1,
                                           DrillBuf buf2, int start2, int scale2, int precision2,
                                           DrillBuf outputBuf, int outputStart, int expectedScale, int expectedPrecision
                                           ) {
    BigDecimal in1 = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromVector(buf1, start1, scale1);
    BigDecimal in2 = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromVector(buf2, start2, scale2);

    BigDecimal output = in1.add(in2);

    // Check for overflow
    if (output.precision() > expectedPrecision) {
      throw new DrillRuntimeException("Overflow error");
    }

    DecimalUtility.writeTwosComplement(output, outputBuf, outputStart, WIDTH);
  }
}
