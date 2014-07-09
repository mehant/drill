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
package org.apache.drill.exec.compile.bytecode;

import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.LocalVariablesSorter;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class HolderReplacingMethodVisitor extends MethodVisitor {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HolderReplacingMethodVisitor.class);

  private final LocalVariablesSorter adder;
  private final IntObjectOpenHashMap<ValueHolderIden.ValueHolderSub> oldToNew = new IntObjectOpenHashMap<>();
  int lastLineNumber = 0;


  public HolderReplacingMethodVisitor(int access, String name, String desc, String signature, MethodVisitor mw) {
    super(Opcodes.ASM4, new LocalVariablesSorter(access, desc, mw));
    this.adder = (LocalVariablesSorter) this.mv;
  }

  @Override
  public void visitTypeInsn(int opcode, String type) {
    ValueHolderIden iden = HOLDERS.get(type);
    if (iden != null) {
      this.sub = iden.getHolderSub(adder);
      this.state = State.REPLACING_CREATE;
    } else {
      super.visitTypeInsn(opcode, type);
    }
  }

  @Override
  public void visitLineNumber(int line, Label start) {
    lastLineNumber = line;
    super.visitLineNumber(line, start);
  }

  @Override
  public void visitVarInsn(int opcode, int var) {
    switch(state){
    case REPLACING_REFERENCE:
    case NORMAL:
      if(oldToNew.containsKey(var)){
        sub = oldToNew.get(var);
        state = State.REPLACING_REFERENCE;
      }else{
        super.visitVarInsn(opcode, var);
      }
      break;
    case REPLACING_CREATE:
      oldToNew.put(var, sub);
      state = State.NORMAL;
      break;
    }
  }

  void directVarInsn(int opcode, int var) {
    mv.visitVarInsn(opcode, var);
  }

  @Override
  public void visitFieldInsn(int opcode, String owner, String name, String desc) {
    switch(state){
    case NORMAL:
      super.visitFieldInsn(opcode, owner, name, desc);
      break;
    case REPLACING_CREATE:
      // noop.
      break;
    case REPLACING_REFERENCE:
      sub.addInsn(name, this, opcode);
      break;
    }
  }

  @Override
  public void visitLocalVariable(String name, String desc, String signature, Label start, Label end, int index) {
//    System.out.println("Local Var: " + desc);
    if (!HOLDER_DESCRIPTORS.contains(desc)) {
      super.visitLocalVariable(name, desc, signature, start, end, index);
    }
  }


  @Override
  public void visitInsn(int opcode) {
    if(state == State.REPLACING_CREATE && opcode == Opcodes.DUP){

    }else{
      super.visitInsn(opcode);
    }
  }

  @Override
  public void visitMethodInsn(int opcode, String owner, String name, String desc) {
    switch(state){
    case NORMAL:
      super.visitMethodInsn(opcode, owner, name, desc);
      break;
    case REPLACING_CREATE:
      // noop.
      break;
    case REPLACING_REFERENCE:
      throw new IllegalStateException(String.format("Holder types are not allowed to be passed between methods.  Ran across problem attempting to invoke method '%s' on line number %d", name, lastLineNumber));
    }
  }

  private static String desc(Class<?> c) {
    Type t = Type.getType(c);
    return t.getDescriptor();
  }

  static {
    Class<?>[] CLASSES = { //
    VarCharHolder.class, //
        NullableVarCharHolder.class, //
        NullableIntHolder.class //
    };

    ImmutableMap.Builder<String, ValueHolderIden> builder = ImmutableMap.builder();
    ImmutableSet.Builder<String> setB = ImmutableSet.builder();
    for (Class<?> c : CLASSES) {
      String desc = desc(c);
      setB.add(desc);
      desc = desc.substring(1, desc.length() - 1);
      System.out.println(desc);

      builder.put(desc, new ValueHolderIden(c));
    }
    HOLDER_DESCRIPTORS = setB.build();
    HOLDERS = builder.build();
  }

  private final static ImmutableMap<String, ValueHolderIden> HOLDERS;
  private final static ImmutableSet<String> HOLDER_DESCRIPTORS;

}
