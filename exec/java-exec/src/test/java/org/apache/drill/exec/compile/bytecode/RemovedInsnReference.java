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

import java.util.Map;

import org.apache.drill.exec.compile.bytecode.ValueHolderIden.ValueHolderSub;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;

public class RemovedInsnReference extends AbstractInsnNode {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RemovedInsnReference.class);

  private int localVar;
  private ValueHolderSub sub;

  protected RemovedInsnReference(int localVar, ValueHolderSub sub) {
    super(Opcodes.ASM4);
    this.localVar = localVar;
  }

  public ValueHolderSub getSub(){
    return sub;
  }

  public int getLocalVar() {
    return localVar;
  }

  @Override
  public int getType() {
    return -1;
  }

  @Override
  public void accept(MethodVisitor cv) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AbstractInsnNode clone(Map labels) {
    throw new UnsupportedOperationException();
  }
}
