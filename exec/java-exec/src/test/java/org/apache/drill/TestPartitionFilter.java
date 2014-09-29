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

import org.apache.drill.common.util.TestTools;
import org.junit.Test;

public class TestPartitionFilter extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestPartitionFilter.class);

  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  @Test  // basic test with dir0 and dir1 filters in different orders
  public void testPartitionFilter1() throws Exception {
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/parquet` where dir0=1994 and dir1='Q1'", TEST_RES_PATH);
    String query2 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/parquet` where dir1='Q1' and dir0=1994", TEST_RES_PATH);
    String query3 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/json` where dir0=1994 and dir1='Q1'", TEST_RES_PATH);
    String query4 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/json` where dir1='Q1' and dir0=1994", TEST_RES_PATH);
    test(query1);
    test(query2);
    test(query3);
    test(query4);
  }

  @Test // partition filters are combined with regular columns in an AND
  public void testPartitionFilter2() throws Exception {
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/parquet` where o_custkey < 1000 and dir0=1994 and dir1='Q1'", TEST_RES_PATH);
    String query2 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/parquet` where dir1='Q1' and o_custkey < 1000 and dir0=1994", TEST_RES_PATH);
    String query3 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/json` where o_custkey < 1000 and dir0=1994 and dir1='Q1'", TEST_RES_PATH);
    String query4 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/json` where dir1='Q1' and o_custkey < 1000 and dir0=1994", TEST_RES_PATH);
    test(query1);
    test(query2);
    test(query3);
    test(query4);
  }

  @Test // partition filters are ANDed and belong to a top-level OR
  public void testPartitionFilter3() throws Exception {
    String query1 = String.format("select * from dfs_test.`%s/multilevel/parquet` where (dir0=1994 and dir1='Q1' and o_custkey < 500) or (dir0=1995 and dir1='Q2' and o_custkey > 500)", TEST_RES_PATH);
    String query2 = String.format("select * from dfs_test.`%s/multilevel/json` where (dir0=1994 and dir1='Q1' and o_custkey < 500) or (dir0=1995 and dir1='Q2' and o_custkey > 500)", TEST_RES_PATH);
    test(query1);
    // test(query2);
  }

  @Test // filters contain join conditions and partition filters
  public void testPartitionFilter4() throws Exception {
    String query1 = String.format("select t1.dir0, t1.dir1, t1.o_custkey, t1.o_orderdate, cast(t2.c_name as varchar(10)) from dfs_test.`%s/multilevel/parquet` t1, cp.`tpch/customer.parquet` t2 where t1.o_custkey = t2.c_custkey and t1.dir0=1994 and t1.dir1='Q1'", TEST_RES_PATH);
    String query2 = String.format("select t1.dir0, t1.dir1, t1.o_custkey, t1.o_orderdate, cast(t2.c_name as varchar(10)) from dfs_test.`%s/multilevel/json` t1, cp.`tpch/customer.parquet` t2 where t1.o_custkey = t2.c_custkey and t1.dir0=1994 and t1.dir1='Q1'", TEST_RES_PATH);
    test(query1);
    test(query2);
  }

}
