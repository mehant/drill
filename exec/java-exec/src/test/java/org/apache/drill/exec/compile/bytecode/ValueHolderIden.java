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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.LocalVariablesSorter;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.google.common.collect.Lists;

class ValueHolderIden {

  final ObjectIntOpenHashMap<String> fieldMap;
  final Type[] types;

  public ValueHolderIden(Class<?> c) {
    Field[] fields = c.getFields();
    List<Field> fldList = Lists.newArrayList();
    for(Field f : fields){
      if(!Modifier.isStatic(f.getModifiers())) {
        fldList.add(f);
      }
    }

    this.types = new Type[fldList.size()];
    fieldMap = new ObjectIntOpenHashMap<String>();
    int i =0;
    for(Field f : fldList){
      types[i] = Type.getType(f.getType());
      fieldMap.put(f.getName(), i);
      i++;
    }
  }

  public ValueHolderSub getHolderSub(LocalVariablesSorter adder) {
    int first = -1;
    for (int i = 0; i < types.length; i++) {
      int varIndex = adder.newLocal(types[i]);
      if (i == 0) {
        first = varIndex;
      }
    }

    return new ValueHolderSub(first);

  }

  public class ValueHolderSub {
    private final int first;

    public ValueHolderSub(int first) {
      assert first != -1 : "Create Holder for sub that doesn't have any fields.";
      this.first = first;
    }

    private int field(String name, InstructionModifier mv) {
      if (!fieldMap.containsKey(name)) throw new IllegalArgumentException(String.format("Unknown name '%s' on line %d.", name, mv.lastLineNumber));
      return fieldMap.lget();
    }

    public void addInsn(String name, InstructionModifier mv, int opcode) {
      switch (opcode) {
      case Opcodes.GETFIELD:
        addKnownInsn(name, mv, Opcodes.ILOAD);
        return;

      case Opcodes.PUTFIELD:
        addKnownInsn(name, mv, Opcodes.ISTORE);
      }
    }

    private void addKnownInsn(String name, InstructionModifier mv, int analogOpcode) {
      int f = field(name, mv);
      Type t = types[f];
      mv.directVarInsn(t.getOpcode(analogOpcode), first + f);
    }

  }


}