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

import org.apache.drill.exec.compile.bytecode.ValueHolderIden.ValueHolderSub;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.LocalVariablesSorter;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.analysis.Frame;

import com.carrotsearch.hppc.IntObjectOpenHashMap;

public class InstructionModifier extends MethodVisitor {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InstructionModifier.class);

  private final IntObjectOpenHashMap<ValueHolderIden.ValueHolderSub> oldToNew = new IntObjectOpenHashMap<>();
  private DirectSorter adder;
  int lastLineNumber = 0;
  private TrackingInstructionList list;

  public InstructionModifier(int access, String name, String desc, String signature, String[] exceptions, TrackingInstructionList list, MethodVisitor inner) {
    super(Opcodes.ASM4, new DirectSorter(access, desc, new CachingMethodVisitor(access, name, desc, signature, exceptions, inner)));
    this.list = list;
    this.adder = (DirectSorter) mv;
  }

  public void setList(TrackingInstructionList list) {
    this.list = list;
  }

  private ReplacingBasicValue popCurrent(){
    // for vararg, we could try to pop an empty stack.  TODO: handle this better.
    if(list.currentFrame.getStackSize() == 0) return null;

    Object o = list.currentFrame.pop();
    if(o instanceof ReplacingBasicValue) return (ReplacingBasicValue) o;
    return null;
  }

  private ReplacingBasicValue getReturn(){
    Object o = list.nextFrame.getStack(list.nextFrame.getStackSize() - 1);
    if(o instanceof ReplacingBasicValue) return (ReplacingBasicValue) o;
    return null;
  }


  @Override
  public void visitInsn(int opcode) {
    switch(opcode){
    case Opcodes.DUP:
      if(popCurrent() != null) return;
    }
    super.visitInsn(opcode);
  }

  @Override
  public void visitTypeInsn(int opcode, String type) {
    ReplacingBasicValue r = getReturn();
    if(r != null){
//      super.visitTypeInsn(opcode, type);
      ValueHolderSub sub = r.getIden().getHolderSub(adder);
      oldToNew.put(r.getIndex(), sub);
    }else{
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
    if(opcode == Opcodes.ASTORE && popCurrent() != null){
      // noop.
    }else if(opcode == Opcodes.ALOAD && getReturn() != null){
      // noop.
    }else{
      super.visitVarInsn(opcode, var);
    }

  }

  void directVarInsn(int opcode, int var) {
    adder.directVarInsn(opcode, var);
  }

  @Override
  public void visitFieldInsn(int opcode, String owner, String name, String desc) {

    switch(opcode){
    case Opcodes.PUTFIELD:
      // pop twice for put.
      popCurrent();

    case Opcodes.GETFIELD:
      // pop again.
      ReplacingBasicValue v = popCurrent();
      if(v != null){
//        super.visitFieldInsn(opcode, owner, name, desc);
        ValueHolderSub sub = oldToNew.get(v.getIndex());
        sub.addInsn(name, this, opcode);
        return;
      }
    }

    super.visitFieldInsn(opcode, owner, name, desc);
  }

  @Override
  public void visitLocalVariable(String name, String desc, String signature, Label start, Label end, int index) {
//    if (!ReplacingInterpreter.HOLDER_DESCRIPTORS.contains(desc)) {
//      super.visitLocalVariable(name, desc, signature, start, end, index);
//    }
  }



  @Override
  public void visitMethodInsn(int opcode, String owner, String name, String desc) {
    int len = Type.getArgumentTypes(desc).length;

    ReplacingBasicValue obj = popCurrent();

    if(obj != null && opcode != Opcodes.INVOKESTATIC){
      return;
    }

    for(int i =0; i < len; i++){
      ReplacingBasicValue v = popCurrent();
      if(v != null){
        throw new IllegalStateException(String.format("Holder types are not allowed to be passed between methods.  Ran across problem attempting to invoke method '%s' on line number %d", name, lastLineNumber));
      }
    }
    super.visitMethodInsn(opcode, owner, name, desc);
  }


}
