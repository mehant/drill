package org.apache.drill.exec.compile.bytecode;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.tree.MethodNode;

public class CachingMethodVisitor extends MethodNode{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CachingMethodVisitor.class);

  MethodVisitor inner;

  public CachingMethodVisitor(int access, String name, String desc, String signature, String[] exceptions, MethodVisitor v) {
    super(access, name, desc, signature, exceptions);
    this.inner = v;
  }

  @Override
  public void visitEnd() {
    super.visitEnd();
    accept(inner);
  }


}
