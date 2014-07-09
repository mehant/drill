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
package org.apache.drill.exec.physical.impl.filter;

import org.apache.drill.exec.record.selection.SelectionVector2;

public class Vect {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Vect.class);

  public static final int get(SelectionVector2  v, TwoByteVariable index){
    return v.getIndex(i(index));
  }

  public static final void set(SelectionVector2  v, int index, OneByteVariable val){
    v.setIndex(index, i(val));
  }

  public static final void set(SelectionVector2  v, TwoByteVariable index, int val){
    v.setIndex(i(index), val);
  }

  public static final void set(SelectionVector2  v, int index, int val){
    v.setIndex( index , val);
  }


  public static Loop incLoop(int start, int end){
    return new LoopImpl(start, end);
  }

  private interface Variable {}
  public interface OneByteVariable extends Variable {}
  public interface TwoByteVariable extends Variable {}
  public interface FourByteVariable extends Variable {}

  public interface Loop {
    public Variable var(ValueVector v);
    public boolean condition();
  }

  private static int i(Variable v){
    return ((LoopImpl.VarImpl)v).index;
  }

  private static class LoopImpl implements Loop {
    private final int end;
    private int current = 0;
    private final VarImpl var = new VarImpl();

    public LoopImpl(int start, int end) {
      super();
      this.current = start;
      this.end = end;
    }

    private class VarImpl implements OneByteVariable, TwoByteVariable, FourByteVariable{
      int index;
    }


    @Override
    public OneByteVariable one() {
      return var;
    }

    @Override
    public TwoByteVariable two() {
      return var;
    }

    @Override
    public FourByteVariable four() {
      return var;
    }

    @Override
    public boolean condition() {
      return current++ < end;
    }

  }
}
