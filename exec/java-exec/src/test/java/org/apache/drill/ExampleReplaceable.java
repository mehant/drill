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

  public int two(int inIndex) {
    NullableVarCharHolder left = new NullableVarCharHolder();
//    left.start = 1;
//    left.end = 2;
//    left.buffer = null;
//    left.isSet = 1;
//    NullableVarCharHolder left = out3;
    VarCharHolder right = constant5;
    if (left.end - left.start == right.end - right.start) {

    }

    return 0;
  }

}
