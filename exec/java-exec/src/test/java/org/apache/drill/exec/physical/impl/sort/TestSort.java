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
package org.apache.drill.exec.physical.impl.sort;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.util.JsonStringHashMap;
import org.junit.Test;

/**
 * Placeholder for all sort related test. Can be used as we move
 * more tests to use the new test framework
 */
public class TestSort extends BaseTestQuery {

  static JsonStringHashMap x = new JsonStringHashMap();
  static JsonStringArrayList<JsonStringHashMap> repeated_map = new JsonStringArrayList<>();

  static {
    x.put("c", 1l);
    repeated_map.add(0, x);
  }

  @Test
  public void testSortWithComplexInput() throws Exception {

    testBuilder()
        .sqlQuery("select (t.a) as col from cp.`jsoninput/repeatedmap_sort_bug.json` t order by t.b")
        .optionSettingQueriesForTestQuery("alter session set `planner.slice_target` = 1")
        .ordered()
        .baselineColumns("col")
        .baselineValues(repeated_map)
        .build().run();

    test("alter session set `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
  }


}