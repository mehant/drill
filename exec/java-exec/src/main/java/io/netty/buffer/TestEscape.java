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
package io.netty.buffer;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import org.apache.drill.exec.expr.holders.BitHolder;

public class TestEscape {



  public static class Timer{
    long n1;
    String name;
    public Timer(String name){
      this.n1 = System.nanoTime();
      this.name = name;
    }

    public void print(long sum){
      System.out.println(String.format("Completed %s in %d ms.  Output was %d", name, (System.nanoTime() - n1)/1000/1000, sum));
    }
  }

  public static Timer time(String name){
    return new Timer(name);
  }

  public static void main(String args[]){
    TestEscape et = new TestEscape();
    Monkey m = new Monkey();
    for(int i =0; i < 10; i++){
      time("get alloc").print(et.getAlloc(m));
    }
  }

  public long getAlloc(Monkey m){
    long sum = 0;
    for(int i =0; i < 490000000; i++){
      RR r = new RR();
      m.get(i, i+1, r);
      sum += r.v1 + r.v2;
    }
    return sum;
  }


  public static class Ad{
    long x;
    long y;
    public Ad(long x, long y) {
      super();
      this.x = x;
      this.y = y;
    }
  }


  public static final class EH{
    int index;
    int value;
    public EH(int index, int value) {
      super();
      this.index = index;
      this.value = value;
    }
  }

  public static final class RR{
    int v1;
    int v2;

    public RR(){

    }
    public RR(int v1, int v2) {
      super();
      this.v1 = v1;
      this.v2 = v2;
    }
  }

  public long add(long a, long b){
    return a + b;
  }


  public final static class Monkey{
    final IntBuffer buf;

    public Monkey(){
      ByteBuffer bb = ByteBuffer.allocateDirect(Integer.MAX_VALUE);
      buf = bb.asIntBuffer();
    }

    public final void set(int index, int value){
      buf.put(index, value);
    }

    public final void set(EH a){
      buf.put(a.index, a.value);
    }

    public final int getV1(int index){
      return buf.get(index);
    }

    public final int getV2(int index){
      return buf.get(index);
    }

    public final void get(int index1, int index2, BitHolder h){
      h.value = buf.get(index1) < buf.get(index2) ? 0 : 1;
    }

    public final RR get(int index1, int index2){
      return new RR(buf.get(index1), buf.get(index2));
    }

    public final void get(int index1, int index2, RR rr){
      rr.v1 = buf.get(index1);
      rr.v2 = buf.get(index2);
    }

  }

}
