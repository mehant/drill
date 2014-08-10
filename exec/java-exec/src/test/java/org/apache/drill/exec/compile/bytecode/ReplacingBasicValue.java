package org.apache.drill.exec.compile.bytecode;

import org.objectweb.asm.Type;
import org.objectweb.asm.tree.analysis.BasicValue;

public class ReplacingBasicValue extends BasicValue{

  ValueHolderIden iden;
  int index;

  public ReplacingBasicValue(Type type, ValueHolderIden iden, int index) {
    super(type);
    this.index = index;
    this.iden = iden;
  }

  public ValueHolderIden getIden() {
    return iden;
  }

  public int getIndex() {
    return index;
  }


}
