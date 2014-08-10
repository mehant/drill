package org.apache.drill.exec.compile.bytecode;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InsnList;
import org.objectweb.asm.tree.analysis.Frame;

public class TrackingInstructionList extends InsnList {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TrackingInstructionList.class);

  Frame<?> currentFrame;
  Frame<?> nextFrame;
  Frame<?>[] frames;
  InsnList inner;
  int index = 0;



  public TrackingInstructionList(Frame<?>[] frames, InsnList inner) {
    super();

    this.frames = frames;
    this.inner = inner;
  }

  public InsnList getInner(){
    return inner;
  }

  @Override
  public void accept(MethodVisitor mv) {
    AbstractInsnNode insn = inner.getFirst();
    while (insn != null) {
        currentFrame = frames[index];
        nextFrame = index +1 < frames.length ? frames[index+1] : null;
        insn.accept(mv);

        insn = insn.getNext();
        index++;
    }
  }


  @Override
  public int size() {
    return inner.size();
  }




}
