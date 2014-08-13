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

import io.netty.buffer.DrillBuf;
import io.netty.util.internal.PlatformDependent;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueHolderHelper;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

public final class TestFilter {
  final NullableVarCharVector vv0;
  final VarCharHolder string4;

  final VarCharHolder constant5;
  final NullableVarCharVector.Accessor vv0a;
  final long laddr;
  final long raddr;
  int constant5_start;
  int constant5_end;
  DrillBuf constant5_buffer;

  private static final int MAX = 1 << 16;
  private static final int INNER_LOOP = 20000;

  private static class TimeHolder {
    final String desc;
    final long[] samples;
    final int[] outputs;
    int pos = 0;
    final Stopwatch watch = new Stopwatch();
    final Stopwatch watch2 = new Stopwatch();

    public TimeHolder(int sampleCount, String desc) {
      this.desc = desc;
      this.samples = new long[sampleCount];
      this.outputs = new int[sampleCount];
      this.watch2.start();
    }

    public void start() {
      watch.start();
    }

    public void stop(int val) {
      watch.stop();
      samples[pos] = watch.elapsed(TimeUnit.MICROSECONDS);
      outputs[pos] = val;
      pos++;
      watch.reset();
    }

    public void print() {
      watch2.stop();
      Arrays.sort(samples);
      Arrays.sort(outputs);
      int cnt = 0;
      long total = 0;
      final int samplesToRemove = (int) (samples.length * 0.2);
      for (int i = samplesToRemove; i < samples.length - samplesToRemove; i++) {
        total += samples[i];
        cnt++;
      }
      long outSum = 0;
      for (int o : outputs) {
        outSum += o;
      }
      long totalMillis = watch2.elapsed(TimeUnit.MILLISECONDS);
      System.out.println((total / cnt) + "micros, " + totalMillis + " total ms. " + outSum + ", " + desc);
    }
  }

  public static void main(String[] args) throws Exception {
    TopLevelAllocator a = new TopLevelAllocator();
    TestFilter f = new TestFilter(a);

    {
      int x = 0;
      TimeHolder h = new TimeHolder(INNER_LOOP, "separate compare method with direct addressing");
      for (int z = 0; z < INNER_LOOP; z++) {
        h.start();
        x = f.x(x);
        h.stop(x);
      }
      h.print();
    }
    {
      int x = 0;
      TimeHolder h = new TimeHolder(INNER_LOOP, "original-bytecode");
      for (int z = 0; z < INNER_LOOP; z++) {
        h.start();
        x = f.unrewrite(x);
        h.stop(x);
      }
      h.print();
    }
    {
      int x = 0;
      TimeHolder h = new TimeHolder(INNER_LOOP, "bytecode-rewritten");
      for (int z = 0; z < INNER_LOOP; z++) {
        h.start();
        x = f.rewrite(x);
        h.stop(x);
      }
      h.print();
    }

    {
      int x = 0;
      TimeHolder h = new TimeHolder(INNER_LOOP, "separate compare method with indirect addressing");
      for (int z = 0; z < INNER_LOOP; z++) {
        h.start();
        x = f.x3(x);
        h.stop(x);
      }
      h.print();
    }

    {
      int x = 0;
      TimeHolder h = new TimeHolder(INNER_LOOP, "combined method with indirect addressing");
      for (int z = 0; z < INNER_LOOP; z++) {
        h.start();
        x = f.x2(x);
        h.stop(x);
      }
      h.print();
    }

    {
      int x = 0;
      TimeHolder h = new TimeHolder(INNER_LOOP, "combined method with direct addressing");
      for (int z = 0; z < INNER_LOOP; z++) {
        h.start();
        x = f.x4(x);
        h.stop(x);
      }
      h.print();
    }

    {
      int x = 0;
      TimeHolder h = new TimeHolder(INNER_LOOP, "original loop with updated bytebuf references and disabled checks");
      for (int z = 0; z < INNER_LOOP; z++) {
        h.start();
        x = f.originalLoop(x);
        h.stop(x);
      }
      h.print();
    }

    {
      int x = 0;
      TimeHolder h = new TimeHolder(INNER_LOOP, "original loop++ (oeval4)");
      for (int z = 0; z < INNER_LOOP; z++) {
        h.start();
        x = f.originalLoop2(x);
        h.stop(x);
      }
      h.print();
    }

    {
      int x = 0;
      TimeHolder h = new TimeHolder(INNER_LOOP, "original loop++ (oeval5) ");
      for (int z = 0; z < INNER_LOOP; z++) {
        h.start();
        x = f.originalLoop3(x);
        h.stop(x);
      }
      h.print();
    }
//
    {
      int x = 0;
      TimeHolder h = new TimeHolder(INNER_LOOP, "separate method loop separate from doEval, with indirect addressing");
      for (int z = 0; z < INNER_LOOP; z++) {
        h.start();
        x = f.loop(x);
        h.stop(x);
      }
      h.print();
    }

    {
      int x = 0;
      TimeHolder h = new TimeHolder(INNER_LOOP,
          "combined method with direct addressing (char comparison and outer loop)");
      for (int z = 0; z < INNER_LOOP; z++) {
        h.start();
        x = f.x5(x);
        h.stop(x);
      }
      h.print();
    }

    {
      int x = 0;
      TimeHolder h = new TimeHolder(INNER_LOOP,
          "separate methods with direct addressing (char comparison and outer loop)");
      for (int z = 0; z < INNER_LOOP; z++) {
        h.start();
        x = f.sepCompareWithDoubleDirect(x);
        h.stop(x);
      }
      h.print();
    }
  }

  public int loop(int x) {
    for (int i = 0; i < MAX; i++) {
      if (doEval(i, 0)) {
        x += i;
      } else {
        x -= i;
      }
    }
    return x;
  }

  private int x(int x) {
    final VarCharHolder constant5 = this.constant5;
    final long laddr = this.laddr;
    final long raddr = this.raddr;
    final long oaddr = vv0.getOffsetAddr();
    final int cStart = constant5.start;
    final int cEnd = constant5.end;
    final NullableVarCharVector.Accessor vv0a = this.vv0a;
    final long bAddr = vv0.getBitAddr();

    for (int inIndex = 0; inIndex < MAX; inIndex++) {

      boolean outSet = PlatformDependent.getByte(bAddr + inIndex) == 1;
      // boolean outSet = vv0a.isSet((inIndex)) == 1;
      if (outSet) {
        // long startEnd = PlatformDependent.getLong(oaddr + inIndex);

        long startEnd = vv0a.getStartEnd(inIndex);
        boolean b = compare2((int) (startEnd >> 32), (int) startEnd, cStart, cEnd, laddr, raddr);
        if (b) {
          x += inIndex;
        } else {
          x -= inIndex;
        }
      } else {
        x -= inIndex;
      }
    }
    return x;
  }

  public static final boolean compareX2(NullableVarCharVector v, int inIndex, int r, final int rEnd, final long raddr) {
    long bitAddr = v.getBitAddr();
    long offsetAddr = v.getOffsetAddr();
    final long laddr= v.getDataAddr();

    // final AccountingByteBuf lb = left.buffer;
    // final AccountingByteBuf rb = right.buffer;
    boolean outSet = PlatformDependent.getByte(bitAddr + inIndex) == 1;
    if(!outSet) return false;
    long startEnd = PlatformDependent.getLong(offsetAddr + inIndex);
    int l = (int) (startEnd >> 32);
    int lEnd = (int) startEnd;

    int n = lEnd - l;
    if (n == rEnd - r) {
      long lPos = laddr + l;
      long rPos = raddr + r;

      while (n > 7) {
        long leftLong = PlatformDependent.getLong(lPos);
        long rightLong = PlatformDependent.getLong(rPos);
        if (leftLong != rightLong) {
          return false;
        }
        lPos += 8;
        rPos += 8;
        n -= 8;
      }
      while (n-- != 0) {
        byte leftByte = PlatformDependent.getByte(lPos);
        byte rightByte = PlatformDependent.getByte(rPos);
        if (leftByte != rightByte) {
          return false;
        }
        lPos++;
        rPos++;
      }
      return true;
    } else {
      return false;
    }
  }


  public static final boolean compareX3(NullableVarCharVector v, int inIndex, int r, final int rEnd, final long raddr) {
    long bitAddr = v.getBitAddr();
    long offsetAddr = v.getOffsetAddr();
    final long laddr= v.getDataAddr();

    // final AccountingByteBuf lb = left.buffer;
    // final AccountingByteBuf rb = right.buffer;
    boolean outSet = PlatformDependent.getByte(bitAddr + inIndex) == 1;
    if(!outSet) return false;
    long startEnd = PlatformDependent.getLong(offsetAddr + inIndex);
    int l = (int) (startEnd >> 32);
    int lEnd = (int) startEnd;

    int n = lEnd - l;
    if (n == rEnd - r) {
      long lPos = laddr + l;
      long rPos = raddr + r;

      while (n > 7) {
        long leftLong = PlatformDependent.getLong(lPos);
        long rightLong = PlatformDependent.getLong(rPos);
        if (leftLong != rightLong) {
          return false;
        }
        lPos += 8;
        rPos += 8;
        n -= 8;
      }
      while (n-- != 0) {
        byte leftByte = PlatformDependent.getByte(lPos);
        byte rightByte = PlatformDependent.getByte(rPos);
        if (leftByte != rightByte) {
          return false;
        }
        lPos++;
        rPos++;
      }
      return true;
    } else {
      return false;
    }
  }

  public static final boolean compareX(long bitAddr, long offsetAddr, int inIndex, int r, final int rEnd, final long laddr, final long raddr) {
    // final AccountingByteBuf lb = left.buffer;
    // final AccountingByteBuf rb = right.buffer;
    boolean outSet = PlatformDependent.getByte(bitAddr + inIndex) == 1;
    if(!outSet) return false;
    long startEnd = PlatformDependent.getLong(offsetAddr + inIndex);
    int l = (int) (startEnd >> 32);
    int lEnd = (int) startEnd;

    int n = lEnd - l;
    if (n == rEnd - r) {
      long lPos = laddr + l;
      long rPos = raddr + r;

      while (n > 7) {
        long leftLong = PlatformDependent.getLong(lPos);
        long rightLong = PlatformDependent.getLong(rPos);
        if (leftLong != rightLong) {
          return false;
        }
        lPos += 8;
        rPos += 8;
        n -= 8;
      }
      while (n-- != 0) {
        byte leftByte = PlatformDependent.getByte(lPos);
        byte rightByte = PlatformDependent.getByte(rPos);
        if (leftByte != rightByte) {
          return false;
        }
        lPos++;
        rPos++;
      }
      return true;
    } else {
      return false;
    }
  }
  private int sepCompareWithDoubleDirect(int x) {
    final VarCharHolder constant5 = this.constant5;
    final long laddr = this.laddr;
    final long raddr = this.raddr;
    final long oaddr = vv0.getOffsetAddr();
    final int cStart = constant5.start;
    final int cEnd = constant5.end;
    final long bAddr = vv0.getBitAddr();

    final long end = bAddr + MAX;
    long b = bAddr;
    long o = oaddr;
    int inIndex = 0;
    for (; b < end; b++, o+=4, inIndex++) {

      boolean outSet = PlatformDependent.getByte(b) == 1;
      // boolean outSet = vv0a.isSet((inIndex)) == 1;
      if (outSet) {
        long startEnd = PlatformDependent.getLong(o);

//        long startEnd = vv0a.getStartEnd(inIndex);
        boolean c = compare2((int) (startEnd >> 32), (int) startEnd, cStart, cEnd, laddr, raddr);
        if (c) {
          x += inIndex;
        } else {
          x -= inIndex;
        }
      } else {
        x -= inIndex;
      }
    }
    return x;
  }

  private int x3(int x) {
    final VarCharHolder constant5 = this.constant5;
    final long laddr = this.laddr;
    final long raddr = this.raddr;
    final long oaddr = vv0.getOffsetAddr();
    final int cStart = constant5.start;
    final int cEnd = constant5.end;
    final NullableVarCharVector.Accessor vv0a = this.vv0a;
    final long bAddr = vv0.getBitAddr();

    for (int inIndex = 0; inIndex < MAX; inIndex++) {

      boolean outSet = PlatformDependent.getByte(bAddr + inIndex) == 1;
      // boolean outSet = vv0a.isSet((inIndex)) == 1;
      if (outSet) {
        // long startEnd = PlatformDependent.getLong(oaddr + inIndex);

        long startEnd = vv0a.getStartEnd(inIndex);
        boolean b = compare((int) (startEnd >> 32), (int) startEnd, cStart, cEnd, laddr, raddr);
        if (b) {
          x += inIndex;
        } else {
          x -= inIndex;
        }
      } else {
        x -= inIndex;
      }
    }
    return x;
  }

  private int x2(int x) {
    final VarCharHolder constant5 = this.constant5;
    final long laddr = this.laddr;
    final long raddr = this.raddr;
    final long oaddr = vv0.getOffsetAddr();
    final int cStart = constant5.start;
    final int cEnd = constant5.end;
    final NullableVarCharVector.Accessor vv0a = this.vv0a;
    final long bAddr = vv0.getBitAddr();

    for (int inIndex = 0; inIndex < MAX; inIndex++) {

      boolean outSet = PlatformDependent.getByte(bAddr + inIndex) == 1;
      // boolean outSet = vv0a.isSet((inIndex)) == 1;
      if (outSet) {
        long startEnd = PlatformDependent.getLong(oaddr + inIndex);

        // long startEnd = vv0a.getStartEnd(inIndex);

        int l = (int) (startEnd >> 32);
        int lEnd = (int) startEnd;
        int r = cStart;
        // /////
        boolean out;
        blk: {
          int n = lEnd - l;
          if (n == cEnd - cStart) {
            while (n > 7) {
              long leftLong = PlatformDependent.getLong(laddr + l);
              long rightLong = PlatformDependent.getLong(raddr + l);
              l += 8;
              if (leftLong != rightLong) {
                out = false;
                break blk;
              }
              n -= 8;
            }
            while (n-- != 0) {
              byte leftByte = PlatformDependent.getByte(laddr + (l++));
              byte rightByte = PlatformDependent.getByte(raddr + (r++));
              if (leftByte != rightByte) {
                out = false;
                break blk;
              }
            }
            out = true;
            break blk;
          } else {
            out = false;
            break blk;
          }
        }
        // /////
        // boolean b = compare( (int) (startEnd >> 32), (int) startEnd, cStart, cEnd, laddr, raddr);
        // /////int l, int r, final int lEnd, final int rEnd, final long laddr, final long raddr
        if (out) {
          x += inIndex;
        } else {
          x -= inIndex;
        }
      } else {
        x -= inIndex;
      }
    }
    return x;
  }

  private int x4(int x) {
    final VarCharHolder constant5 = this.constant5;
    final long laddr = this.laddr;
    final long raddr = this.raddr;
    final long oaddr = vv0.getOffsetAddr();
    final int cStart = constant5.start;
    final int cEnd = constant5.end;
    final NullableVarCharVector.Accessor vv0a = this.vv0a;
    final long bAddr = vv0.getBitAddr();

    for (int inIndex = 0; inIndex < MAX; inIndex++) {

      boolean outSet = PlatformDependent.getByte(bAddr + inIndex) == 1;
      // boolean outSet = vv0a.isSet((inIndex)) == 1;
      if (outSet) {
        long startEnd = PlatformDependent.getLong(oaddr + inIndex * 4);

        // long startEnd = vv0a.getStartEnd(inIndex);

        final int lStart = (int) (startEnd >> 32);
        int lEnd = (int) startEnd;
        long r = raddr + cStart;

        // /////
        boolean out;
        blk: {
          int n = lEnd - lStart;
          if (n == cEnd - cStart) {
            long l = laddr + lStart;
            while (n > 7) {
              long leftLong = PlatformDependent.getLong(l);
              long rightLong = PlatformDependent.getLong(r);
              if (leftLong != rightLong) {
                out = false;
                break blk;
              }
              l += 8;
              r += 8;
              n -= 8;
            }
            while (n-- != 0) {
              byte leftByte = PlatformDependent.getByte(l);
              byte rightByte = PlatformDependent.getByte(r);
              if (leftByte != rightByte) {
                out = false;
                break blk;
              }
              l++;
              r++;
            }
            out = true;
            break blk;
          } else {
            out = false;
            break blk;
          }
        }
        // /////
        // boolean b = compare( (int) (startEnd >> 32), (int) startEnd, cStart, cEnd, laddr, raddr);
        // /////int l, int r, final int lEnd, final int rEnd, final long laddr, final long raddr
        if (out) {
          x += inIndex;
        } else {
          x -= inIndex;
        }
      } else {
        x -= inIndex;
      }
    }
    return x;
  }

  private int x5(int x) {
    final VarCharHolder constant5 = this.constant5;
    final long laddr = this.laddr;
    final long raddr = this.raddr;
    final long oaddr = vv0.getOffsetAddr();
    final int cStart = constant5.start;
    final int cEnd = constant5.end;
    final long bAddr = vv0.getBitAddr();

    long setPointer = bAddr;
    long startPointer = oaddr;
    for (int inIndex = 0; inIndex < MAX; inIndex++, setPointer++, startPointer += 4) {

      boolean outSet = PlatformDependent.getByte(setPointer) == 1;
      // boolean outSet = vv0a.isSet((inIndex)) == 1;
      if (outSet) {
        long startEnd = PlatformDependent.getLong(startPointer);

        // long startEnd = vv0a.getStartEnd(inIndex);

        final int lStart = (int) (startEnd >> 32);
        int lEnd = (int) startEnd;
        long r = raddr + cStart;

        // /////
        boolean out;
        blk: {
          int n = lEnd - lStart;
          if (n == cEnd - cStart) {
            long l = laddr + lStart;
            while (n > 7) {
              long leftLong = PlatformDependent.getLong(l);
              long rightLong = PlatformDependent.getLong(r);
              if (leftLong != rightLong) {
                out = false;
                break blk;
              }
              l += 8;
              r += 8;
              n -= 8;
            }
            while (n-- != 0) {
              byte leftByte = PlatformDependent.getByte(l);
              byte rightByte = PlatformDependent.getByte(r);
              if (leftByte != rightByte) {
                out = false;
                break blk;
              }
              l++;
              r++;
            }
            out = true;
            break blk;
          } else {
            out = false;
            break blk;
          }
        }
        // /////
        // boolean b = compare( (int) (startEnd >> 32), (int) startEnd, cStart, cEnd, laddr, raddr);
        // /////int l, int r, final int lEnd, final int rEnd, final long laddr, final long raddr
        if (out) {
          x += inIndex;
        } else {
          x -= inIndex;
        }
      } else {
        x -= inIndex;
      }
    }
    return x;
  }

  public TestFilter(TopLevelAllocator a) {
    vv0 = new NullableVarCharVector(MaterializedField.create("a", Types.optional(MinorType.VARCHAR)), a);
    vv0.allocateNew(Integer.MAX_VALUE, MAX);
    vv0.getMutator().generateTestData(MAX);
    vv0a = vv0.getAccessor();
    string4 = ValueHolderHelper.getVarCharHolder(a, "James Compagno");
    constant5 = string4;
    constant5_start = constant5.start;
    constant5_end = constant5.end;
    constant5_buffer = constant5.buffer;

    laddr = vv0.getData().memoryAddress();
    raddr = string4.buffer.memoryAddress() + string4.start;
  }

  public boolean doEval(final int inIndex, final int outIndex) {
    final VarCharHolder constant5 = this.constant5;

    boolean outSet = vv0a.isSet((inIndex)) == 1;
    if (outSet) {
      long startEnd = vv0a.getStartEnd(inIndex);
      return compare2((int) (startEnd >> 32), (int) startEnd, constant5.start, constant5.end, laddr, raddr);
    } else {
      return false;
    }
  }

  public static final boolean compare(int l, int r, final int lEnd, final int rEnd, final long laddr, final long raddr) {
    // final AccountingByteBuf lb = left.buffer;
    // final AccountingByteBuf rb = right.buffer;

    int n = lEnd - l;
    if (n == rEnd - r) {
      while (n > 7) {
        long leftLong = PlatformDependent.getLong(laddr + l);
        long rightLong = PlatformDependent.getLong(raddr + l);
        if (leftLong != rightLong) {
          return false;
        }
        l += 8;
        n -= 8;
      }
      while (n-- != 0) {
        byte leftByte = PlatformDependent.getByte(laddr + (l));
        byte rightByte = PlatformDependent.getByte(raddr + (l));
        if (leftByte != rightByte) {
          return false;
        }
        l++;
      }
      return true;
    } else {
      return false;
    }
  }

  public static final boolean compare2(int l, int r, final int lEnd, final int rEnd, final long laddr, final long raddr) {
    // final AccountingByteBuf lb = left.buffer;
    // final AccountingByteBuf rb = right.buffer;

    int n = lEnd - l;
    if (n == rEnd - r) {
      long lPos = laddr + l;
      long rPos = raddr + r;

      while (n > 7) {
        long leftLong = PlatformDependent.getLong(lPos);
        long rightLong = PlatformDependent.getLong(rPos);
        if (leftLong != rightLong) {
          return false;
        }
        lPos += 8;
        rPos += 8;
        n -= 8;
      }
      while (n-- != 0) {
        byte leftByte = PlatformDependent.getByte(lPos);
        byte rightByte = PlatformDependent.getByte(rPos);
        if (leftByte != rightByte) {
          return false;
        }
        lPos++;
        rPos++;
      }
      return true;
    } else {
      return false;
    }
  }

  public static final int compare2i(int l, int r, final int lEnd, final int rEnd, final long laddr, final long raddr) {
    // final AccountingByteBuf lb = left.buffer;
    // final AccountingByteBuf rb = right.buffer;

    int n = lEnd - l;
    if (n == rEnd - r) {
      long lPos = laddr + l;
      long rPos = raddr + r;

      while (n > 7) {
        long leftLong = PlatformDependent.getLong(lPos);
        long rightLong = PlatformDependent.getLong(rPos);
        if (leftLong != rightLong) {
          return 0;
        }
        lPos += 8;
        rPos += 8;
        n -= 8;
      }
      while (n-- != 0) {
        byte leftByte = PlatformDependent.getByte(lPos);
        byte rightByte = PlatformDependent.getByte(rPos);
        if (leftByte != rightByte) {
          return 0;
        }
        lPos++;
        rPos++;
      }
      return 1;
    } else {
      return 0;
    }
  }

  public boolean doEval2(final int inIndex, final int outIndex) {
    final NullableVarCharHolder x1 = new NullableVarCharHolder();
    final NullableVarCharVector.Accessor vv0a = this.vv0a;
    final VarCharHolder string4 = this.string4;
    final VarCharHolder constant5 = this.constant5;
    final long oAddr = vv0.getOffsetAddr();

    {
      NullableVarCharHolder out3 = x1;
      out3.isSet = vv0a.isSet((inIndex));
      if (out3.isSet == 1) {
        {
          long startEnd = PlatformDependent.getLong(oAddr + inIndex);
          // long startEnd = vv0a.getStartEnd(inIndex);
          out3.start = (int) (startEnd >> 32);
          out3.end = (int) startEnd;
        }
      }
      // ---- start of eval portion of equal function. ----//
      boolean out6;
      {
        if (out3.isSet == 0) {
          out6 = false;
        } else {
          boolean out;
          NullableVarCharHolder left = out3;
          VarCharHolder right = constant5;
          GCompareVarCharVarChar$EqualsVarCharVarChar_eval: {
            outside: {
              // final AccountingByteBuf lb = left.buffer;
              // final AccountingByteBuf rb = right.buffer;

              int n = left.end - left.start;
              if (n == right.end - right.start) {
                int l = left.start;
                int r = right.start;
                while (n > 7) {
                  long leftLong = PlatformDependent.getLong(laddr + l);
                  long rightLong = PlatformDependent.getLong(raddr + l);
                  if (leftLong != rightLong) {
                    out = false;
                    break outside;
                  }
                  r += 8;
                  l += 8;
                  n -= 8;
                }
                while (n-- != 0) {
                  byte leftByte = PlatformDependent.getByte(laddr + (l));
                  byte rightByte = PlatformDependent.getByte(raddr + (r));
                  if (leftByte != rightByte) {
                    out = false;
                    break outside;
                  }
                  l++;
                  r++;
                }
                out = true;
              } else {
                out = false;
              }
            }
          }
          out = true;
          out6 = out;
          out = true;
        }
      }
      // ---- end of eval portion of equal function. ----//
      return out6;
    }
  }

  public boolean originalEval(int inIndex, int outIndex) {
    {
      NullableVarCharHolder out3 = new NullableVarCharHolder();
      out3.isSet = vv0.getAccessor().isSet((inIndex));
      if (out3.isSet == 1) {
        {
          vv0.getAccessor().get((inIndex), out3);
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
          out.isSet = 1;
        }
      }
      // ---- end of eval portion of equal function. ----//
      return (out6.value == 1);
    }
  }

  public boolean originalEval2(int inIndex, int outIndex) {
    {
      DrillBuf out3_buffer = null;
      int out3_start = 0;
      int out3_end = 0;
      int out3_isSet = 0;

      out3_isSet = vv0a.isSet((inIndex));
      if (out3_isSet == 1) {
        {
          out3_buffer = vv0.getData();
          long startEnd = vv0a.getStartEnd(inIndex);
          out3_start = (int) (startEnd >> 32);
          out3_end = (int) (startEnd);
        }
      }
      // ---- start of eval portion of equal function. ----//
      int out6_isSet = 0;
      int out6_value = 0;
      {
        if (out3_isSet == 0) {
          out6_isSet = 0;
        } else {
          int out_isSet = 0;
          int out_value = 0;

          int right_start = constant5.start;
          int right_end = constant5.end;
          DrillBuf right_buffer = constant5.buffer;

          GCompareVarCharVarChar$EqualsVarCharVarChar_eval: {
            outside: {
              int n = out3_end - out3_start;
              if (n == right_end - right_start) {
                int l = out3_start;
                int r = right_start;
                while (n-- != 0) {
                  byte leftByte = out3_buffer.getByte(l);
                  byte rightByte = right_buffer.getByte(r);
                  if (leftByte != rightByte) {
                    out_value = 0;
                    break outside;
                  }
                  l++;
                  r++;
                }
                out_value = 1;
              } else {
                out_value = 0;
              }
            }
          }
          out_isSet = 1;
          out6_isSet = out_isSet;
          out6_value = out_value;
          out_isSet = 1;
        }
      }
      // ---- end of eval portion of equal function. ----//
      return (out6_value == 1);
    }
  }

  public boolean originalEval3(int inIndex, int outIndex) {
//    return compareX(vv0.getBitAddr(), vv0.getOffsetAddr(), inIndex, constant5.start, constant5.end, vv0.getDataAddr(), constant5.buffer.memoryAddress());
    return compareX2(vv0, inIndex, constant5.start, constant5.end, constant5.buffer.memoryAddress());
  }

  public boolean originalEval4(int inIndex, int outIndex) {
    int outSet = TestFilter.getIsSet(vv0, inIndex);
    if(! (outSet == 1) ) return false;
    long startEnd = TestFilter.getStartEnd(vv0, inIndex);

    // accessor
//  int outSet = vv0a.isSet(inIndex);
//  long startEnd = vv0a.getStartEnd(inIndex);

// direct
//  int outSet = PlatformDependent.getByte(vv0.getBitAddr() + inIndex);
//    long startEnd = PlatformDependent.getLong(vv0.getOffsetAddr() + (inIndex*4));
    return compare2((int) (startEnd >> 32), (int) startEnd, constant5.start, constant5.end, laddr, raddr);
  }

  public static int getIsSet(NullableVarCharVector v, int index){
    return PlatformDependent.getByte(v.getBitAddr() + index);
  }
  public static long getStartEnd(NullableVarCharVector v, int inIndex){
    return PlatformDependent.getLong(v.getOffsetAddr() + (inIndex*4));
  }

  public boolean originalEval5(int inIndex, int outIndex) {
    int outSet = vv0a.isSet(inIndex);
    if(! (outSet == 1) ) return false;
    long startEnd = vv0a.getStartEnd(inIndex);
//    long startEnd = PlatformDependent.getLong(vv0.getOffsetAddr() + inIndex);
    return compare2((int) (startEnd >> 32), (int) startEnd, constant5.start, constant5.end, laddr, raddr);
  }


  public int originalLoop(int x) {
    int svIndex = 0;
    for (int i = 0; i < MAX; i++) {
      if (originalEval(i, 0)) {
        x += i;
      } else {
        x -= i;

      }
    }
    return x;

  }

  public int originalLoop2(int x) {
    int svIndex = 0;
    for (int i = 0; i < MAX; i++) {
      if (originalEval4(i, 0)) {
        x += i;
      } else {
        x -= i;

      }
    }
    return x;

  }

  public int originalLoop3(int x) {
    int svIndex = 0;
    for (int i = 0; i < MAX; i++) {
      if (originalEval5(i, 0)) {
        x += i;
      } else {
        x -= i;

      }
    }
    return x;

  }

  public int unrewrite(int x) throws SchemaChangeException {
    int svIndex = 0;
    for (int i = 0; i < MAX; i++) {
      if (doEvalUnrewrite(i, 0)) {
        x += i;
      } else {
        x -= i;

      }
    }
    return x;

  }

  public int rewrite(int x) throws SchemaChangeException {
    int svIndex = 0;
    for (int i = 0; i < MAX; i++) {
      if (doEvalRewritten(i, 0)) {
        x += i;
      } else {
        x -= i;

      }
    }
    return x;

  }

  public boolean doEvalUnrewrite(int inIndex, int outIndex) throws SchemaChangeException {
    {
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
          out.isSet = 1;
        }
      }
      // ---- end of eval portion of equal function. ----//
      return (out6.value == 1);
    }
  }



public boolean doEvalRewritten2(int paramInt1, int paramInt2)
    throws SchemaChangeException
  {
    int i = 0; int j = 0; int k = 0; DrillBuf localDrillBuf = null;
    i = this.vv0.getAccessor().isSet(paramInt1);
    if (i == 1)
    {
      localDrillBuf = this.vv0.getData();
      long l = this.vv0.getAccessor().getStartEnd(paramInt1);
      j = (int)(l >> 32);
      k = (int)l;
    }

    int m = 0; int n = 0;

    if (i == 0) {
      m = 0;
    } else {
      int i1 = 0; int i2 = 0;

      VarCharHolder localVarCharHolder = this.constant5;

      if (k - j == localVarCharHolder.end - localVarCharHolder.start) {
        int i3 = k - j;
        int i4 = j;
        int i5 = localVarCharHolder.start;
        while (i3-- != 0) {
          int i6 = localDrillBuf.getByte(i4++);
          int i7 = localVarCharHolder.buffer.getByte(i5++);
          if (i6 != i7) {
            i2 = 0;
            break;
          }
        }
        i2 = 1;
      } else {
        i2 = 0;
      }

      i1 = 1;
      m = i1; n = i2;
      m = 1;
    }

    return n == 1;
  }

public boolean doEvalRewritten(int paramInt1, int paramInt2) throws SchemaChangeException {
    int i = 0; int j = 0; int k = 0; DrillBuf localDrillBuf = null;
    i = this.vv0.getAccessor().isSet(paramInt1);
//    System.out.print(i);
    if (i == 1) {
      localDrillBuf = this.vv0.getData();
      long l = this.vv0.getAccessor().getStartEnd(paramInt1);
      j = (int)(l >> 32);
      k = (int)l;
    }

    int m = 0; int n = 0;

    if (i != 0) {
      n = compare2i(j, k, constant5.start, constant5.end, localDrillBuf.memoryAddress(), constant5.buffer.memoryAddress());
    }

    return n == 1;
  }
}