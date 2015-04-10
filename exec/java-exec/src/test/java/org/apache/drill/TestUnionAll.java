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
package org.apache.drill;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;
import org.apache.drill.exec.work.foreman.UnsupportedRelOperatorException;
import org.junit.Test;

public class TestUnionAll extends BaseTestQuery{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestUnionAll.class);

  @Test  // Simple Union-All over two scans
  public void testUnionAll1() throws Exception {
    String query = "(select n_regionkey from cp.`tpch/nation.parquet`) union all (select r_regionkey from cp.`tpch/region.parquet`)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testUnionAllQueries/q1.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("n_regionkey")
        .build().run();
  }

  @Test  // Union-All over inner joins
  public void testUnionAll2() throws Exception {
    String query =
         "select n1.n_nationkey from cp.`tpch/nation.parquet` n1 inner join cp.`tpch/region.parquet` r1 on n1.n_regionkey = r1.r_regionkey where n1.n_nationkey in (1, 2) " +
         "union all " +
         "select n2.n_nationkey from cp.`tpch/nation.parquet` n2 inner join cp.`tpch/region.parquet` r2 on n2.n_regionkey = r2.r_regionkey where n2.n_nationkey in (3, 4)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testUnionAllQueries/q2.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("n_nationkey")
        .build().run();
  }

  @Test  // Union-All over grouped aggregates
  public void testUnionAll3() throws Exception {
    String query = "select n1.n_nationkey from cp.`tpch/nation.parquet` n1 where n1.n_nationkey in (1, 2) group by n1.n_nationkey union all select r1.r_regionkey from cp.`tpch/region.parquet` r1 group by r1.r_regionkey";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testUnionAllQueries/q3.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("n_nationkey")
        .build().run();
  }

  @Test    // Chain of Union-Alls
  public void testUnionAll4() throws Exception {
    String query = "select n_regionkey from cp.`tpch/nation.parquet` union all select r_regionkey from cp.`tpch/region.parquet` union all select n_nationkey from cp.`tpch/nation.parquet` union all select c_custkey from cp.`tpch/customer.parquet` where c_custkey < 5";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testUnionAllQueries/q4.tsv")
        .baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("n_regionkey")
        .build().run();
  }

  @Test  // Union-All of all columns in the table
  public void testUnionAll5() throws Exception {
    String query = "select r_name, r_comment, r_regionkey from cp.`tpch/region.parquet` r1 " +
                     "union all " +
                     "select r_name, r_comment, r_regionkey from cp.`tpch/region.parquet` r2";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testUnionAllQueries/q5.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.INT)
        .baselineColumns("r_name", "r_comment", "r_regionkey")
        .build().run();
  }

  @Test // Union-All where same column is projected twice in right child
  public void testUnionAll6() throws Exception {
    String query = "select n_nationkey, n_regionkey from cp.`tpch/nation.parquet` where n_regionkey = 1 union all select r_regionkey, r_regionkey from cp.`tpch/region.parquet` where r_regionkey = 2";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testUnionAllQueries/q6.tsv")
        .baselineTypes(TypeProtos.MinorType.INT, TypeProtos.MinorType.INT)
        .baselineColumns("n_nationkey", "n_regionkey")
        .build().run();
  }

  @Test // Union-All where same column is projected twice in left and right child
  public void testUnionAll6_1() throws Exception {
    String query = "select n_nationkey, n_nationkey from cp.`tpch/nation.parquet` union all select r_regionkey, r_regionkey from cp.`tpch/region.parquet`";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testUnionAllQueries/q6_1.tsv")
        .baselineTypes(TypeProtos.MinorType.INT, TypeProtos.MinorType.INT)
        .baselineColumns("n_nationkey", "n_nationkey0")
        .build().run();
  }

  @Test  // Union-all of two string literals of different lengths
  public void testUnionAll7() throws Exception {
    String query = "select 'abc' from cp.`tpch/region.parquet` union all select 'abcdefgh' from cp.`tpch/region.parquet`";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testUnionAllQueries/q7.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("EXPR$0")
        .build().run();
  }

  @Test  // Union-all of two character columns of different lengths
  public void testUnionAll8() throws Exception {
    String query = "select n_name, n_nationkey from cp.`tpch/nation.parquet` union all select r_comment, r_regionkey  from cp.`tpch/region.parquet`";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testUnionAllQueries/q8.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.INT)
        .baselineColumns("n_name", "n_nationkey")
        .build().run();
  }

  @Test // DRILL-1905: Union-all of * column from JSON files in different directories
  public void testUnionAll9() throws Exception {
    String file0 = FileUtils.getResourceAsFile("/multilevel/json/1994/Q1/orders_94_q1.json").toURI().toString();
    String file1 = FileUtils.getResourceAsFile("/multilevel/json/1995/Q1/orders_95_q1.json").toURI().toString();
    String query = String.format("select o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, o_orderkey from dfs_test.`%s` union all " +
                                 "select o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, o_orderkey from dfs_test.`%s`", file0, file1);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testUnionAllQueries/q9.tsv")
        .baselineTypes(TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.FLOAT8, TypeProtos.MinorType.VARCHAR,
                       TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.BIGINT,TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.BIGINT)
        .baselineColumns("o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate",
                         "o_orderpriority", "o_clerk", "o_shippriority", "o_comment", "o_orderkey")
        .build().run();
  }

  @Test // Union All constant literals
  public void testUnionAll10() throws Exception {
    String query = "(select n_name, 'LEFT' as LiteralConstant, n_nationkey, '1' as NumberConstant from cp.`tpch/nation.parquet`) " +
              "union all " +
              "(select 'RIGHT', r_name, '2', r_regionkey from cp.`tpch/region.parquet`)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testUnionAllQueries/q10.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.INT, TypeProtos.MinorType.INT)
        .baselineColumns("n_name", "LiteralConstant", "n_nationkey", "NumberConstant")
        .build().run();
  }

  @Test
  public void testUnionAllViewExpandableStar() throws Exception {
    test("use dfs_test.tmp");
    test("create view nation_view_testunionall as select n_name, n_nationkey from cp.`tpch/nation.parquet`;");
    test("create view region_view_testunionall as select r_name, r_regionkey from cp.`tpch/region.parquet`;");

    String query1 = "(select * from dfs_test.tmp.`nation_view_testunionall`) " +
                    "union all " +
                    "(select * from dfs_test.tmp.`region_view_testunionall`) ";

    String query2 =  "(select r_name, r_regionkey from cp.`tpch/region.parquet`) " +
                     "union all " +
                     "(select * from dfs_test.tmp.`nation_view_testunionall`)";

    try {
      testBuilder()
          .sqlQuery(query1)
          .unOrdered()
          .csvBaselineFile("testframework/testUnionAllQueries/q11.tsv")
          .baselineTypes(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.INT)
          .baselineColumns("n_name", "n_nationkey")
          .build().run();

      testBuilder()
          .sqlQuery(query2)
          .unOrdered()
          .csvBaselineFile("testframework/testUnionAllQueries/q12.tsv")
          .baselineTypes(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.INT)
          .baselineColumns("r_name", "r_regionkey")
          .build().run();
    } finally {
      test("drop view nation_view_testunionall");
      test("drop view region_view_testunionall");
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see DRILL-2002
  public void testUnionAllViewUnExpandableStar() throws Exception {
    test("use dfs_test.tmp");
    test("create view nation_view_testunionall as select * from cp.`tpch/nation.parquet`;");

    try {
      String query = "(select * from dfs_test.tmp.`nation_view_testunionall`) " +
                     "union all (select * from cp.`tpch/region.parquet`)";
      test(query);
    } catch(Exception ex) {
      SqlUnsupportedException.errorMessageToException(ex.getMessage());
      throw ex;
    } finally {
      test("drop view nation_view_testunionall");
    }
  }

  @Test
  public void testDiffDataTypesAndModes() throws Exception {
    test("use dfs_test.tmp");
    test("create view nation_view_testunionall as select n_name, n_nationkey from cp.`tpch/nation.parquet`;");
    test("create view region_view_testunionall as select r_name, r_regionkey from cp.`tpch/region.parquet`;");

    String t1 = "(select n_comment, n_regionkey from cp.`tpch/nation.parquet` limit 5)";
    String t2 = "(select * from nation_view_testunionall  limit 5)";
    String t3 = "(select full_name, store_id from cp.`employee.json` limit 5)";
    String t4 = "(select * from region_view_testunionall  limit 5)";

    String query1 = t1 + " union all " + t2 + " union all " + t3 + " union all " + t4;

    try {
      testBuilder()
          .sqlQuery(query1)
          .unOrdered()
          .csvBaselineFile("testframework/testUnionAllQueries/q13.tsv")
          .baselineTypes(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.BIGINT)
          .baselineColumns("n_comment", "n_regionkey")
          .build().run();
    } finally {
      test("drop view nation_view_testunionall");
      test("drop view region_view_testunionall");
    }
  }

  @Test // see DRILL-2203
  public void testDistinctOverUnionAllwithFullyQualifiedColumnNames() throws Exception {
    String query = "select distinct sq.x1, sq.x2 " +
        "from " +
        "((select n_regionkey as a1, n_name as b1 from cp.`tpch/nation.parquet`) " +
        "union all " +
        "(select r_regionkey as a2, r_name as b2 from cp.`tpch/region.parquet`)) as sq(x1,x2)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testUnionAllQueries/q14.tsv")
        .baselineTypes(TypeProtos.MinorType.INT, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("x1", "x2")
        .build().run();
  }

  @Test // see DRILL-1923
  public void testUnionAllContainsColumnANumericConstant() throws Exception {
    String query = "(select n_nationkey, n_regionkey, n_name from cp.`tpch/nation.parquet`  limit 5) " +
        "union all " +
        "(select 1, n_regionkey, 'abc' from cp.`tpch/nation.parquet` limit 5)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testUnionAllQueries/q15.tsv")
        .baselineTypes(TypeProtos.MinorType.INT, TypeProtos.MinorType.INT, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("n_nationkey", "n_regionkey", "n_name")
        .build().run();
  }

  @Test // see DRILL-2207
  public void testUnionAllEmptySides() throws Exception {
    String query1 = "(select n_nationkey, n_regionkey, n_name from cp.`tpch/nation.parquet`  limit 0) " +
        "union all " +
        "(select 1, n_regionkey, 'abc' from cp.`tpch/nation.parquet` limit 5)";

    String query2 = "(select n_nationkey, n_regionkey, n_name from cp.`tpch/nation.parquet`  limit 5) " +
        "union all " +
        "(select 1, n_regionkey, 'abc' from cp.`tpch/nation.parquet` limit 0)";

    testBuilder()
        .sqlQuery(query1)
        .unOrdered()
        .csvBaselineFile("testframework/testUnionAllQueries/q16.tsv")
        .baselineTypes(TypeProtos.MinorType.INT, TypeProtos.MinorType.INT, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("n_nationkey", "n_regionkey", "n_name")
        .build().run();


    testBuilder()
        .sqlQuery(query2)
        .unOrdered()
        .csvBaselineFile("testframework/testUnionAllQueries/q17.tsv")
        .baselineTypes(TypeProtos.MinorType.INT, TypeProtos.MinorType.INT, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("n_nationkey", "n_regionkey", "n_name")
        .build().run();
    }

  @Test // see DRILL-1977, DRILL-2376, DRILL-2377, DRILL-2378, DRILL-2379
  public void testAggregationOnUnionAllOperator() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/t.json").toURI().toString();
    String query1 = String.format(
        "(select calc1, max(b1) as `max`, min(b1) as `min`, count(c1) as `count` " +
        "from (select a1 + 10 as calc1, b1, c1 from dfs_test.`%s` " +
        "union all " +
        "select a1 + 100 as diff1, b1 as diff2, c1 as diff3 from dfs_test.`%s`) " +
        "group by calc1 order by calc1)", root, root);

    String query2 = String.format(
        "(select calc1, min(b1) as `min`, max(b1) as `max`, count(c1) as `count` " +
        "from (select a1 + 10 as calc1, b1, c1 from dfs_test.`%s` " +
        "union all " +
        "select a1 + 100 as diff1, b1 as diff2, c1 as diff3 from dfs_test.`%s`) " +
        "group by calc1 order by calc1)", root, root);

    testBuilder()
        .sqlQuery(query1)
        .ordered()
        .csvBaselineFile("testframework/testExampleQueries/testAggregationOnUnionAllOperator/q1.tsv")
        .baselineTypes(TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT)
        .baselineColumns("calc1", "max", "min", "count")
        .build().run();

    testBuilder()
        .sqlQuery(query2)
        .ordered()
        .csvBaselineFile("testframework/testExampleQueries/testAggregationOnUnionAllOperator/q2.tsv")
        .baselineTypes(TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.BIGINT)
        .baselineColumns("calc1", "min", "max", "count")
        .build().run();
  }

  @Test(expected = RpcException.class) // see DRILL-2590
  public void testUnionAllImplicitCastingFailure() throws Exception {
    String rootInt = FileUtils.getResourceAsFile("/store/json/intData.json").toURI().toString();
    String rootBoolean = FileUtils.getResourceAsFile("/store/json/booleanData.json").toURI().toString();

    String query = String.format(
        "(select key from dfs_test.`%s` " +
        "union all " +
        "select key from dfs_test.`%s` )", rootInt, rootBoolean);

    test(query);
  }

  @Test // see DRILL-2591
  public void testDateAndTimestampJson() throws Exception {
    String rootDate = FileUtils.getResourceAsFile("/store/json/dateData.json").toURI().toString();
    String rootTimpStmp = FileUtils.getResourceAsFile("/store/json/timeStmpData.json").toURI().toString();

    String query = String.format(
        "(select max(key) as key from dfs_test.`%s` " +
        "union all " +
        "select key from dfs_test.`%s`)", rootDate, rootTimpStmp);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testUnionAllQueries/q18.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("key")
        .build().run();
  }

  @Test // see DRILL-2637
  public void testUnionAllOneInputContainsAggFunction() throws Exception {
    String root = FileUtils.getResourceAsFile("/multilevel/csv/1994/Q1/orders_94_q1.csv").toURI().toString();
    String query1 = String.format("select * from ((select count(c1) as ct from (select columns[0] c1 from dfs.`%s`)) \n" +
        "union all \n" +
        "(select columns[0] c2 from dfs.`%s`)) order by ct limit 3", root, root);

    String query2 = String.format("select * from ((select columns[0] ct from dfs.`%s`)\n" +
        "union all \n" +
        "(select count(c1) as c2 from (select columns[0] c1 from dfs.`%s`))) order by ct limit 3", root, root);

    String query3 = String.format("select * from ((select count(c1) as ct from (select columns[0] c1 from dfs.`%s`))\n" +
        "union all \n" +
        "(select count(c1) as c2 from (select columns[0] c1 from dfs.`%s`))) order by ct", root, root);

    testBuilder()
        .sqlQuery(query1)
        .ordered()
        .baselineColumns("ct")
        .baselineValues((long) 10)
        .baselineValues((long) 66)
        .baselineValues((long) 99)
        .build().run();

    testBuilder()
        .sqlQuery(query2)
        .ordered()
        .baselineColumns("ct")
        .baselineValues((long) 10)
        .baselineValues((long) 66)
        .baselineValues((long) 99)
        .build().run();

    testBuilder()
        .sqlQuery(query3)
         .ordered()
         .baselineColumns("ct")
         .baselineValues((long) 10)
         .baselineValues((long) 10)
         .build().run();
  }

  @Test // see DRILL-2717
  public void testUnionInputsGroupByOnCSV() throws Exception {
    String root = FileUtils.getResourceAsFile("/multilevel/csv/1994/Q1/orders_94_q1.csv").toURI().toString();
    String query = String.format("select * from \n" +
            "((select columns[0] as col0 from dfs.`%s` t1 \n" +
            "where t1.columns[0] = 66) \n" +
            "union all \n" +
            "(select columns[0] c2 from dfs.`%s` t2 \n" +
            "where t2.columns[0] is not null \n" +
            "group by columns[0])) \n" +
        "group by col0"
        , root, root);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col0")
        .baselineValues("290")
        .baselineValues("291")
        .baselineValues("323")
        .baselineValues("352")
        .baselineValues("389")
        .baselineValues("417")
        .baselineValues("66")
        .baselineValues("673")
        .baselineValues("833")
        .baselineValues("99")
        .build().run();
  }
}