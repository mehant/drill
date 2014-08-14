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
package org.apache.drill.exec.nested;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestNestedComplexSchema extends BaseTestQuery {

  @Test
  public void testNested1() throws Exception {
    test("select tbl.arrayval[0] from cp.`nested/nested_1.json` tbl");
  }

  @Test
  public void testNested2() throws Exception {
    test("select tbl.a.arrayval[0] from cp.`nested/nested_2.json` tbl");
  }

  @Test
  public void testNested3() throws Exception {
    test("select tbl.a.arrayval[0].val1[0] from cp.`nested/nested_3.json` tbl");
  }

  /* Test to detect schema changes
   * If schema changes are not detected correctly and the operators do not return OK_NEW_SCHEMA
   * then we will get null for columns that are newly added in a scan batch, hence checking for
   * null and verifying count should be good enough to test if schema change is being detected correctly
   */
  @Test
  public void testDetectSchemaChange() throws Exception {
    // If one of the fields part of the top level repeated map changes schema change is detected correctly
    int count = testSql("select t.a[0].field1_2 from dfs.`[WORKING_PATH]/exec/java-exec/src/test/resources/jsoninput/schemachangetest/*json` as t where isnull(t.a[0].field1_2)=false");
    assert count == 1;
    count = testSql("select t.a[0].field1_4 from dfs.`[WORKING_PATH]/exec/java-exec/src/test/resources/jsoninput/schemachangetest/*json` as t where isnull(t.a[0].field1_4)=false");
    assert count == 1;


    // If one of the fields part of the map within a top level map changes then schema change is detected correctly
    count = testSql("select t.a[0].field1_3.bar from dfs.`[WORKING_PATH]/exec/java-exec/src/test/resources/jsoninput/schemachangetest/*json` as t where isnull(t.a[0].field1_3.bar)=false");
    assert count == 1;
    count = testSql("select t.a[0].field1_3.foo from dfs.`[WORKING_PATH]/exec/java-exec/src/test/resources/jsoninput/schemachangetest/*json` as t where isnull(t.a[0].field1_3.foo)=false");
    assert count == 1;
  }

}
