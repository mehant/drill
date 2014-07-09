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


import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.analysis.Analyzer;
import org.objectweb.asm.tree.analysis.AnalyzerException;
import org.objectweb.asm.tree.analysis.BasicInterpreter;
import org.objectweb.asm.tree.analysis.BasicValue;
import org.objectweb.asm.tree.analysis.Frame;

public class ScalarReplacementNode extends MethodNode {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScalarReplacementNode.class);

  MethodVisitor inner;
  String name;

  public ScalarReplacementNode(String name, MethodVisitor inner){
    this.inner = inner;
  }

  @Override
  public void visitEnd() {
    super.visitEnd();

    // next we'll rewrite instructions.

    Analyzer<BasicValue> a = new Analyzer<>(new BasicInterpreter());
    Frame<BasicValue>[] frames;
    try {
      frames = a.analyze(name, this);
    } catch (AnalyzerException e) {
      throw new IllegalStateException(e);
    }

    // replace stack reference with abstractinsn


    // finally we'll replay against class visitor.
    accept(inner);
  }


  private void doWork(){
    this.instructions.getFirst();
    Frame f = new Frame(5000, 5000);
    f.
  }

}
