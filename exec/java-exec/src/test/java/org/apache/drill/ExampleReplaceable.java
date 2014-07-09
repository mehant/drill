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

import org.apache.drill.exec.expr.holders.NullableIntHolder;

public class ExampleReplaceable {

  public static void main(String[] args){
    ExampleReplaceable r = new ExampleReplaceable();
    System.out.println(r.xyz());
  }

  public static void z(NullableIntHolder h){

  }
  public int xyz(){
//    NullableIntHolder h = new NullableIntHolder();
//    h.value = 4;
//    return h.value;

    System.clearProperty("");

    NullableIntHolder h = new NullableIntHolder();
    NullableIntHolder h2 = new NullableIntHolder();
    h.isSet = 1;
    h.value = 4;
    h2.isSet = 1;
    h2.value = 6;
    if(h.isSet == h2.isSet){
      return h.value + h2.value;
    }else{
      return -1;
    }

  }
}
