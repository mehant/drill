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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.apache.drill.exec.planner.sql.DrillSqlAggOperator;
import org.apache.drill.exec.planner.sql.DrillSqlOperator;
import org.eigenbase.sql.SqlOperator;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Sets;

public class DrillFunctionRegistry {

  /*
   * As a convention drill function names shouldn't have space or special
   * characters in them. '$' is used for some internal functions by Optiq
   * and we use '_' instead of space for multi word functions
   */
  private static final String patterString = "^[\\$a-z0-9_]*$";
  private static final Pattern pattern = Pattern.compile(patterString);

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillFunctionRegistry.class);

  private ArrayListMultimap<String, DrillFuncHolder> methods = ArrayListMultimap.create();

  /* Hash map to prevent registering functions with exactly matching signatures
   * key: Function Name + Input's Major Type
   * Value: Class name where function is implemented
   */
  private HashMap<String, String> functionSignatureMap = new HashMap<>();

  public DrillFunctionRegistry(DrillConfig config) {
    FunctionConverter converter = new FunctionConverter();
    Set<Class<? extends DrillFunc>> providerClasses = PathScanner.scanForImplementations(DrillFunc.class, config.getStringList(ExecConstants.FUNCTION_PACKAGES));
    for (Class<? extends DrillFunc> clazz : providerClasses) {
      DrillFuncHolder holder = converter.getHolder(clazz);
      if (holder != null) {
        // register handle for each name the function can be referred to
        String[] names = holder.getRegisteredNames();

        // Create the string for input types
        String functionInput = "";
        for (DrillFuncHolder.ValueReference ref : holder.parameters) {
          functionInput += ref.getType().toString();
        }
        for (String name : names) {
          String functionName = name.toLowerCase();

          // Dont allow function names with special characters
          if (!pattern.matcher(functionName).matches()) {
            throw new AssertionError(String.format("Drill does not allow function with spaces. Func name: %s, " +
                "Class name: %s ", functionName, clazz.getName()));
          }
          methods.put(functionName, holder);
          String functionSignature = functionName + functionInput;

          String existingImplementation;
          if ((existingImplementation = functionSignatureMap.get(functionSignature)) != null) {
            throw new AssertionError(String.format("Conflicting functions with similar signature found. Func Name: %s, Class name: %s " +
                " Class name: %s", functionName, clazz.getName(), existingImplementation));
          } else {
            functionSignatureMap.put(functionSignature, clazz.getName());
          }
        }
      } else {
        logger.warn("Unable to initialize function for class {}", clazz.getName());
      }
    }
    if (logger.isTraceEnabled()) {
      StringBuilder allFunctions = new StringBuilder();
      for (DrillFuncHolder method: methods.values()) {
        allFunctions.append(method.toString()).append("\n");
      }
      logger.trace("Registered functions: [\n{}]", allFunctions);
    }
  }

  public int size(){
    return methods.size();
  }

  /** Returns functions with given name. Function name is case insensitive. */
  public List<DrillFuncHolder> getMethods(String name) {
    return this.methods.get(name.toLowerCase());
  }

  public void register(DrillOperatorTable operatorTable) {
    SqlOperator op;
    for (Entry<String, Collection<DrillFuncHolder>> function : methods.asMap().entrySet()) {
      Set<Integer> argCounts = Sets.newHashSet();
      String name = function.getKey().toUpperCase();
      for (DrillFuncHolder func : function.getValue()) {
        if (argCounts.add(func.getParamCount())) {
          if (func.isAggregating()) {
            op = new DrillSqlAggOperator(name, func.getParamCount());
          } else {
            op = new DrillSqlOperator(name, func.getParamCount(), func.getReturnType(), func.isDeterministic());
          }
          operatorTable.add(function.getKey(), op);
        }
      }
    }
  }

}
