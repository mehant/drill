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
package org.apache.drill.exec.expr.fn;

import com.google.common.base.Strings;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JPrimitiveType;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;

import java.util.List;
import java.util.Map;

public class DrillSimpleErrFuncHolder extends DrillSimpleFuncHolder {
  public DrillSimpleErrFuncHolder(FunctionTemplate.FunctionScope scope, FunctionTemplate.NullHandling nullHandling, boolean isBinaryCommutative, boolean isRandom, String[] registeredNames, ValueReference[] parameters, ValueReference returnValue, WorkspaceReference[] workspaceVars, Map<String, String> methods, List<String> imports) {
    super(scope, nullHandling, isBinaryCommutative, isRandom, registeredNames, parameters, returnValue, workspaceVars, methods, imports);
  }


  @Override
  protected ClassGenerator.HoldingContainer generateEvalBody(ClassGenerator<?> g, ClassGenerator.HoldingContainer[] inputVariables, String body, JVar[] workspaceJVars) {

    g.getEvalBlock().directStatement(String.format("//---- start of eval portion of %s function. ----//", registeredNames[0]));

    JBlock sub = new JBlock(true, true);
    JBlock topSub = sub;
    ClassGenerator.HoldingContainer out = null;
    TypeProtos.MajorType returnValueType = returnValue.type;

    // Add error code
    JType err = JPrimitiveType.parse(g.getModel(), "int");
    //sub.decl(err, "DRILL_ERROR_CODE", JExpr.lit(0));
    JVar err1 = sub.decl(err, "DRILL_ERROR_CODE", JExpr.lit(0));

    // add outside null handling if it is defined.
    if(nullHandling == FunctionTemplate.NullHandling.NULL_IF_NULL){
      JExpression e = null;
      for(ClassGenerator.HoldingContainer v : inputVariables){
        if(v.isOptional()){
          if(e == null){
            e = v.getIsSet();
          }else{
            e = e.mul(v.getIsSet());
          }
        }
      }

      if(e != null){
        // if at least one expression must be checked, set up the conditional.
        returnValueType = returnValue.type.toBuilder().setMode(TypeProtos.DataMode.OPTIONAL).build();
        out = g.declare(returnValueType);
        e = e.eq(JExpr.lit(0));
        JConditional jc = sub._if(e);
        jc._then().assign(out.getIsSet(), JExpr.lit(0));
        sub = jc._else();
      }
    }

    if(out == null) out = g.declare(returnValueType);

    // add the subblock after the out declaration.
    g.getEvalBlock().add(topSub);


    JVar internalOutput = sub.decl(JMod.FINAL, g.getHolderType(returnValueType), returnValue.name, JExpr._new(g.getHolderType(returnValueType)));
    addProtectedBlock(g, sub, body, inputVariables, workspaceJVars, false);


    JClass t = g.getModel().ref(org.apache.drill.common.exceptions.DrillRuntimeException.class);
    JClass drillErrorType = g.getModel().ref(org.apache.drill.exec.util.DrillFunctionErrors.class);




    // Block if we have an error code returned
    JBlock errBlock = new JBlock();
    JType strType = g.getModel()._ref(String.class);
    JVar errMsg = errBlock.decl(strType, "errorMsg", errBlock.staticInvoke(drillErrorType, "getErrorMsg").arg(err1));
    JConditional condition = errBlock._if(g.getStopOnError().eq(JExpr.lit(true)));
    condition._then()._throw(JExpr._new(t).arg(errMsg));
    condition._else().assign(internalOutput.ref("isSet"), JExpr.lit(0));




    //errBlock._throw(JExpr._new(t).arg(errMsg));
    //sub._if(err1.ne(JExpr.lit(0)))._then().add(errBlock);
    //sub._if(err1.ne(JExpr.lit(0)))._then().add(errBlock);
    //sub._if(err1.ne(JExpr.lit(0)))._then().add(condition);
    //sub._if(err1.ne(JExpr.lit(0)))._then().add(ignoreError).


    //sub.directStatement("if (DRILL_ERROR_CODE != 0) { throw new org.apache.drill.common.exceptions.DrillRuntimeException(org.apache.drill.exec.util.DrillFunctionErrors.getErrorMsg(DRILL_ERROR_CODE)); }");
    //JExpr str = sub.directStatement("org.apache.drill.exec.util.DrillFunctionErrors.getErrorMsg(DRILL_ERROR_CODE);")
    //sub._if(err1.ne(JExpr.lit(0)))._then()._throw(JExpr._new(t).arg("asd"));


    JBlock noErrBlock = new JBlock();
    //noErrBlock.add(noErrBlock.assign(internalOutput.ref("isSet"),JExpr.lit(1)));

    if (sub != topSub) noErrBlock.assign(internalOutput.ref("isSet"),JExpr.lit(1));// Assign null if NULL_IF_NULL mode
    noErrBlock.assign(out.getHolder(), internalOutput);
    if (sub != topSub) noErrBlock.assign(internalOutput.ref("isSet"),JExpr.lit(1));// Assign null if NULL_IF_NULL mode

    JConditional topCond = sub._if(err1.ne(JExpr.lit(0)));
    topCond._then().add(errBlock);
    topCond._else().add(noErrBlock);

    //sub.add(topCond);

    g.getEvalBlock().directStatement(String.format("//---- end of eval portion of %s function. ----//", registeredNames[0]));

    return out;
  }

  @Override
  protected void generateBody(ClassGenerator<?> g, ClassGenerator.BlockType bt, String body, ClassGenerator.HoldingContainer[] inputVariables,
                              JVar[] workspaceJVars, boolean decConstantInputOnly) {
    if (!Strings.isNullOrEmpty(body) && !body.trim().isEmpty()) {
      JBlock sub = new JBlock(true, true);
      JType err = JPrimitiveType.parse(g.getModel(), "int");
      sub.decl(err, "DRILL_ERROR_CODE", JExpr.lit(0));
      if (decConstantInputOnly) {
        addProtectedBlock(g, sub, body, inputVariables, workspaceJVars, true);
      } else {
        addProtectedBlock(g, sub, body, null, workspaceJVars, false);
      }
      //sub.decl(new JPrimitiveType() )
      //JVar err  = JVar(JMod.NONE, JType.parse(g.getModel(), "int"), JExpr.lit(0));


      g.getBlock(bt).directStatement(String.format("/** start %s for function %s **/ ", bt.name(), registeredNames[0]));
      g.getBlock(bt).add(sub);
      g.getBlock(bt).directStatement(String.format("/** end %s for function %s **/ ", bt.name(), registeredNames[0]));

    }
  }

  @Override
  public TypeProtos.MajorType getReturnType(List<LogicalExpression> args) {
    return TypeProtos.MajorType.newBuilder().setMinorType(returnValue.type.getMinorType()).setMode(TypeProtos.DataMode.OPTIONAL).build();
  }

}
