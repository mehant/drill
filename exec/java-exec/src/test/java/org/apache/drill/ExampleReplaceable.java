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
package org.apache.drill;

import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.NullableVarCharVector;

public class ExampleReplaceable {

  static{
    System.exit(1);
  }

  NullableVarCharVector vv0;
  VarCharHolder string4;
  VarCharHolder constant5;

  public static void main(String[] args) {
    ExampleReplaceable r = new ExampleReplaceable();
    System.out.println(r.xyz() + r.two(5));
  }

  public static void x() {
    main(new String[0]);
  }

  public int xyz() {
    NullableIntHolder h = new NullableIntHolder();
    NullableIntHolder h2 = new NullableIntHolder();
    h.isSet = 1;
    h.value = 4;
    h2.isSet = 1;
    h2.value = 6;
    if (h.isSet == h2.isSet) {
      return h.value + h2.value;
    } else {
      return -1;
    }

  }

  public int r(){
    NullableVarCharHolder out3;
    if(Math.abs(0) > 1){
      out3 = new NullableVarCharHolder();
    }else{
      out3 = new NullableVarCharHolder();
    }
    return out3.isSet;
  }

  public int two(int inIndex) {

      NullableVarCharHolder out3 = new NullableVarCharHolder();
      out3.isSet = vv0.getAccessor().isSet((inIndex));
      if (out3.isSet == 1) {
        {
          out3.buffer = vv0.getData();
          long startEnd = vv0.getAccessor().getStartEnd(inIndex);
          out3.start = (int) (startEnd >> 32);
          out3.end = (int) (startEnd);
//          vv0.getAccessor().get((inIndex), out3);
        }
      }
      // ---- start of eval portion of equal function. ----//
      NullableBitHolder out6 = new NullableBitHolder();
      {
        if (out3.isSet == 0) {
          out6.isSet = 0;
        } else {
          final NullableBitHolder out = new NullableBitHolder();
          NullableVarCharHolder left = out3;
          VarCharHolder right = constant5;
          GCompareVarCharVarChar$EqualsVarCharVarChar_eval: {
            outside: {
              if (left.end - left.start == right.end - right.start) {
                int n = left.end - left.start;
                int l = left.start;
                int r = right.start;
                while (n-- != 0) {
                  byte leftByte = left.buffer.getByte(l++);
                  byte rightByte = right.buffer.getByte(r++);
                  if (leftByte != rightByte) {
                    out.value = 0;
                    break outside;
                  }
                }
                out.value = 1;
              } else {
                out.value = 0;
              }
            }
          }
          out.isSet = 1;
          out6 = out;
//          out.isSet = 1;
        }
      }
      // ---- end of eval portion of equal function. ----//
      return out6.value;
    }



}
