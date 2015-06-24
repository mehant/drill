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
package org.apache.drill.exec;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;
import org.apache.drill.exec.work.foreman.UnsupportedFunctionException;
import org.junit.Test;

public class TestWindowFunctions extends BaseTestQuery {
  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  private static void throwAsUnsupportedException(UserException ex) throws Exception {
    SqlUnsupportedException.errorClassNameToException(ex.getOrCreatePBError(false).getException().getExceptionClass());
    throw ex;
  }

  @Test // DRILL-3196
  public void testSinglePartition() throws Exception {
    final String query = "explain plan for select sum(a2) over(partition by a2), count(*) over(partition by a2) \n" +
        "from cp.`tpch/nation.parquet`";

    test(query);
  }

  @Test // DRILL-3196
  public void testSinglePartitionDefinedInWindowList() throws Exception {
    final String query = "explain plan for select sum(a2) over w \n" +
        "from cp.`tpch/nation.parquet` \n" +
        "window w as (partition by a2 order by a2)";

    test(query);
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3196
  public void testMultiplePartitions() throws Exception {
    try {
      final String query = "explain plan for select sum(a2) over(partition by a2), count(*) over(partition by a2,b2,c2) \n" +
          "from cp.`tpch/nation.parquet`";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3196
  public void testSinglePartitionMultipleOrderBy() throws Exception {
    try {
      final String query = "explain plan for select sum(a2) over(partition by a2 order by a2), count(*) over(partition by a2 order by b2) \n" +
          "from cp.`tpch/nation.parquet`";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3196
  public void testMultiplePartitionsDefinedInWindowList() throws Exception {
    try {
      final String query = "explain plan for select sum(a2) over(partition by a2), count(*) over w \n" +
          "from cp.`tpch/nation.parquet` \n" +
          "window w as (partition by a2, b2, c2)";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3182
  public void testWindowFunctionWithDistinct() throws Exception {
    try {
      final String query = "explain plan for select a2, count(distinct b2) over(partition by a2) \n" +
          "from cp.`tpch/nation.parquet`";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3195
  public void testWindowFunctionNTILE() throws Exception {
    try {
      final String query = "explain plan for select NTILE(1) over(partition by n_name order by n_name) \n" +
          "from cp.`tpch/nation.parquet`";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3195
  public void testWindowFunctionLAG() throws Exception {
    try {
      final String query = "explain plan for select LAG(n_nationKey, 1) over(partition by n_name order by n_name) \n" +
          "from cp.`tpch/nation.parquet`";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3195
  public void testWindowFunctionLEAD() throws Exception {
    try {
      final String query = "explain plan for select LEAD(n_nationKey, 1) over(partition by n_name order by n_name) \n" +
          "from cp.`tpch/nation.parquet`";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3195
  public void testWindowFunctionFIRST_VALUE() throws Exception {
    try {
      final String query = "explain plan for select FIRST_VALUE(n_nationKey) over(partition by n_name order by n_name) \n" +
          "from cp.`tpch/nation.parquet`";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3195
  public void testWindowFunctionLAST_VALUE() throws Exception {
    try {
      final String query = "explain plan for select LAST_VALUE(n_nationKey) over(partition by n_name order by n_name) \n" +
          "from cp.`tpch/nation.parquet`";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3188
  public void testWindowFrame() throws Exception {
    try {
      final String query = "select a2, sum(a2) over(partition by a2 order by a2 rows between 1 preceding and 1 following ) \n" +
          "from cp.`tpch/nation.parquet` t \n" +
          "order by a2";

      test(query);
    } catch(UserException ex) {
        throwAsUnsupportedException(ex);
        throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class)  // DRILL-3188
  public void testRowsUnboundedPreceding() throws Exception {
    try {
      final String query = "explain plan for select sum(n_nationKey) over(partition by n_nationKey order by n_nationKey \n" +
      "rows UNBOUNDED PRECEDING)" +
      "from cp.`tpch/nation.parquet` t \n" +
      "order by n_nationKey";

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test // DRILL-3188
  public void testWindowFrameEquivalentToDefault() throws Exception {
    final String query1 = "explain plan for select sum(n_nationKey) over(partition by n_nationKey order by n_nationKey) \n" +
        "from cp.`tpch/nation.parquet` t \n" +
        "order by n_nationKey";

    final String query2 = "explain plan for select sum(n_nationKey) over(partition by n_nationKey order by n_nationKey \n" +
        "range between unbounded preceding and current row) \n" +
        "from cp.`tpch/nation.parquet` t \n" +
        "order by n_nationKey";

    final String query3 = "explain plan for select sum(n_nationKey) over(partition by n_nationKey \n" +
        "rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)" +
        "from cp.`tpch/nation.parquet` t \n" +
        "order by n_nationKey";

    test(query1);
    test(query2);
    test(query3);
  }

  @Test // DRILL-3204
  public void testWindowWithJoin() throws Exception {
    final String query = "select sum(t1.r_regionKey) over(partition by t1.r_regionKey)  \n" +
        "from cp.`tpch/region.parquet` t1, cp.`tpch/nation.parquet` t2 \n" +
        "where t1.r_regionKey = t2.n_nationKey \n" +
        "group by t1.r_regionKey";

    test(query);
  }

  @Test // DRILL-3298
  public void testCountEmptyPartitionByWithExchange() throws Exception {
    String query = String.format("select count(*) over (order by o_orderpriority) as cnt from dfs.`%s/multilevel/parquet` where o_custkey < 100", TEST_RES_PATH);
    try {
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("cnt")
        .optionSettingQueriesForTestQuery("alter session set `planner.slice_target` = 1")
        .baselineValues(1l)
        .baselineValues(4l)
        .baselineValues(4l)
        .baselineValues(4l)
        .build().run();
    } finally {
      test("alter session set `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
    }
  }

  /* Verify the output of aggregate functions (which are reduced
    * eg: avg(x) = sum(x)/count(x)) return results of the correct
    * data type (double)
    */
  @Test
  public void testAvgVarianceWindowFunctions() throws Exception {
    final String avgQuery = "select avg(n_nationkey) over (partition by n_nationkey) col1 " +
        "from cp.`tpch/nation.parquet` " +
        "where n_nationkey = 1";

    testBuilder()
        .sqlQuery(avgQuery)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(1.0d)
        .go();

    final String varianceQuery = "select var_pop(n_nationkey) over (partition by n_nationkey) col1 " +
        "from cp.`tpch/nation.parquet` " +
        "where n_nationkey = 1";

    testBuilder()
        .sqlQuery(varianceQuery)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(0.0d)
        .go();
  }

  @Test
  public void testWindowFunctionWithKnownType() throws Exception {
    final String query = "select sum(cast(col_int as int)) over (partition by col_varchar) as col1 " +
        "from cp.`jsoninput/large_int.json` limit 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(2147483649l)
        .go();

  }
}
