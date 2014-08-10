package org.apache.drill.exec.compile.bytecode;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.commons.LocalVariablesSorter;

public class DirectSorter extends LocalVariablesSorter{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DirectSorter.class);

  public DirectSorter(int access, String desc, MethodVisitor mv) {
    super(access, desc, mv);
  }

  public void directVarInsn(int opcode, int var) {
    mv.visitVarInsn(opcode, var);
  }
}
