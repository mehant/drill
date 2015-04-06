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

package org.apache.drill.exec.physical.impl.join;

import static org.junit.Assert.assertEquals;

import org.apache.drill.PlanTestBase;
import org.apache.drill.common.util.TestTools;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TestNestedLoopJoin extends PlanTestBase {

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(500000);

  private static String nlpattern = "NestedLoopJoin";
  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";
  private static final String NLJ = "Alter session set `planner.enable_hashjoin` = false; " +
      "alter session set `planner.enable_mergejoin` = false; " +
      "alter session set `planner.enable_nljoin_for_scalar_only` = false; ";
  private static final String SINGLE_NLJ = "alter session set `planner.disable_exchanges` = true; " + NLJ;


  // Test queries used by planning and execution tests
  private static final String testNlJoinExists_1 = "select r_regionkey from cp.`tpch/region.parquet` "
      + " where exists (select n_regionkey from cp.`tpch/nation.parquet` "
      + " where n_nationkey < 10)";

  private static final String testNlJoinNotIn_1 = "select r_regionkey from cp.`tpch/region.parquet` "
      + " where r_regionkey not in (select n_regionkey from cp.`tpch/nation.parquet` "
      + "                            where n_nationkey < 4)";

  private static final String testNlJoinInequality_1 = "select r_regionkey from cp.`tpch/region.parquet` "
      + " where r_regionkey > (select min(n_regionkey) from cp.`tpch/nation.parquet` "
      + "                        where n_nationkey < 4)";


  // PLANNING TESTS
  @Test
  public void testNlJoinExists_1_planning() throws Exception {
    testPlanMatchingPatterns(testNlJoinExists_1, new String[]{nlpattern}, new String[]{});
  }

  @Test
  public void testNlJoinNotIn_1_planning() throws Exception {
    testPlanMatchingPatterns(testNlJoinNotIn_1, new String[]{nlpattern}, new String[]{});
  }

  @Test
  public void testNlJoinInequality_1_planning() throws Exception {
    testPlanMatchingPatterns(testNlJoinInequality_1, new String[]{nlpattern}, new String[]{});
  }

  @Test
  public void testNlJoinAggrs_1() throws Exception {
    String query = "select total1, total2 from "
       + "(select sum(l_quantity) as total1 from cp.`tpch/lineitem.parquet` where l_suppkey between 100 and 200), "
       + "(select sum(l_quantity) as total2 from cp.`tpch/lineitem.parquet` where l_suppkey between 200 and 300)  ";
    testPlanMatchingPatterns(query, new String[]{nlpattern}, new String[]{});
  }

  @Test // equality join and scalar right input, hj and mj disabled
  public void testNlJoinEqualityScalar_1() throws Exception {
    String query = "select r_regionkey from cp.`tpch/region.parquet` "
        + " where r_regionkey = (select min(n_regionkey) from cp.`tpch/nation.parquet` "
        + "                        where n_nationkey < 10)";
    test("alter session set `planner.enable_hashjoin` = false");
    test("alter session set `planner.enable_mergejoin` = false");
    testPlanMatchingPatterns(query, new String[]{nlpattern}, new String[]{});
  }

  @Test // equality join and scalar right input, hj and mj disabled, enforce exchanges
  public void testNlJoinEqualityScalar_2() throws Exception {
    String query = "select r_regionkey from cp.`tpch/region.parquet` "
        + " where r_regionkey = (select min(n_regionkey) from cp.`tpch/nation.parquet` "
        + "                        where n_nationkey < 10)";
    test("alter session set `planner.slice_target` = 1");
    test("alter session set `planner.enable_hashjoin` = false");
    test("alter session set `planner.enable_mergejoin` = false");
    testPlanMatchingPatterns(query, new String[]{nlpattern, "BroadcastExchange"}, new String[]{});
  }

  @Test // equality join and non-scalar right input, hj and mj disabled
  public void testNlJoinEqualityNonScalar_1() throws Exception {
    String query = "select r.r_regionkey from cp.`tpch/region.parquet` r inner join cp.`tpch/nation.parquet` n"
        + " on r.r_regionkey = n.n_regionkey where n.n_nationkey < 10";
    test("alter session set `planner.enable_hashjoin` = false");
    test("alter session set `planner.enable_mergejoin` = false");
    test("alter session set `planner.enable_nljoin_for_scalar_only` = false");
    testPlanMatchingPatterns(query, new String[]{nlpattern}, new String[]{});
  }

  @Test // equality join and non-scalar right input, hj and mj disabled, enforce exchanges
  public void testNlJoinEqualityNonScalar_2() throws Exception {
    // String query = "select r.r_regionkey from cp.`tpch/region.parquet` r inner join cp.`tpch/nation.parquet` n"
    //  + " on r.r_regionkey = n.n_regionkey where n.n_nationkey < 10";
    String query = String.format("select n.n_nationkey from cp.`tpch/nation.parquet` n, "
        + " dfs_test.`%s/multilevel/parquet` o "
        + " where n.n_regionkey = o.o_orderkey and o.o_custkey < 5", TEST_RES_PATH);
    test("alter session set `planner.slice_target` = 1");
    test("alter session set `planner.enable_hashjoin` = false");
    test("alter session set `planner.enable_mergejoin` = false");
    test("alter session set `planner.enable_nljoin_for_scalar_only` = false");
    testPlanMatchingPatterns(query, new String[]{nlpattern, "BroadcastExchange"}, new String[]{});
  }


  // EXECUTION TESTS
  @Test
  public void testNLJNonScalar_exec() throws Exception {

    // Simple NLJ between employee and department
    testBuilder()
        .sqlQuery("select t1.employee_id from cp.`employee.json` t1, cp.`department.json` t2 where t1.department_id = t2.department_id")
        .unOrdered()
        .optionSettingQueriesForTestQuery("alter session set `planner.enable_nljoin_for_scalar_only` = false")
        .sqlBaselineQuery("select t1.employee_id from cp.`employee.json` t1, cp.`department.json` t2 where t1.department_id = t2.department_id")
        .optionSettingQueriesForBaseline("alter session set `planner.enable_nljoin_for_scalar_only` = true; ")
        .unOrdered()
        .go();

    // NLJ between employee and department but both tables span over multiple files
    String query = String.format("select t1.employee_id, t1.department_id, t2.department_id from dfs_test.`%s/join/e` t1, dfs_test.`%s/join/d` t2 where t1.department_id = t2.department_id", TEST_RES_PATH, TEST_RES_PATH);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .optionSettingQueriesForTestQuery("alter session set `planner.enable_nljoin_for_scalar_only` = false")
        .sqlBaselineQuery(query)
        .optionSettingQueriesForBaseline("alter session set `planner.enable_nljoin_for_scalar_only` = true; ")
        .unOrdered()
        .go();

    // similar test as above with order by
    query = String.format("select t1.employee_id, t1.department_id, t2.department_id from dfs_test.`%s/join/e` t1, dfs_test.`%s/join/d` t2 where t1.department_id = t2.department_id order by t1.employee_id", TEST_RES_PATH, TEST_RES_PATH);
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .optionSettingQueriesForTestQuery("alter session set `planner.enable_nljoin_for_scalar_only` = false")
        .sqlBaselineQuery(query)
        .optionSettingQueriesForBaseline("alter session set `planner.enable_nljoin_for_scalar_only` = true; ")
        .ordered()
        .go();
  }

  @Test
  public void testNlJoinExists_1_exec() throws Exception {
    testBuilder()
        .sqlQuery(testNlJoinExists_1)
        .unOrdered()
        .baselineColumns("r_regionkey")
        .baselineValues(0)
        .baselineValues(1)
        .baselineValues(2)
        .baselineValues(3)
        .baselineValues(4)
        .go();
  }

  @Test
  public void testNlJoinNotIn_1_exec() throws Exception {
    testBuilder()
        .sqlQuery(testNlJoinNotIn_1)
        .unOrdered()
        .baselineColumns("r_regionkey")
        .baselineValues(2)
        .baselineValues(3)
        .baselineValues(4)
        .go();
  }

  @Test
  public void testNlJoinInequality_1_exec() throws Exception {
    test(testNlJoinInequality_1);
    testBuilder()
        .sqlQuery(testNlJoinInequality_1)
        .unOrdered()
        .baselineColumns("r_regionkey")
        .baselineValues(1)
        .baselineValues(2)
        .baselineValues(3)
        .baselineValues(4)
        .go();
  }


  // TPCH SINGLE MODE TESTS WITH NLJ
  private void testTpchNonDistributeNLJ(String fileName) throws Exception {
    String query = getFile(fileName);
    query = SINGLE_NLJ + query;
    test(query);
  }

  @Test
  public void tpch04() throws Exception{
    testTpchNonDistributeNLJ("queries/tpch/04.sql");
  }

  @Test
  public void tpch05() throws Exception{
    testTpchNonDistributeNLJ("queries/tpch/05.sql");
  }

  @Test
  public void tpch06() throws Exception{
    testTpchNonDistributeNLJ("queries/tpch/06.sql");
  }

  @Test
  public void tpch09() throws Exception{
    testTpchNonDistributeNLJ("queries/tpch/09.sql");
  }
}
