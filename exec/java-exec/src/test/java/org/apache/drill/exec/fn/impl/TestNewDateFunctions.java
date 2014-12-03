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
package org.apache.drill.exec.fn.impl;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestNewDateFunctions extends BaseTestQuery {
  @Test
  public void testUnixTimeStampForDate() throws Exception {
    testBuilder()
        .sqlQuery("select unix_timestamp('2009-03-20 11:30:01') from cp.`employee.json` limit 1")
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValues(Long.parseLong("1237573801"))
        .build().run();

    testBuilder()
        .sqlQuery("select unix_timestamp('2014-08-09 05:15:06') from cp.`employee.json` limit 1")
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValues(Long.parseLong("1407586506"))
        .build().run();

    testBuilder()
        .sqlQuery("select unix_timestamp('1970-01-01 00:00:00') from cp.`employee.json` limit 1")
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValues(Long.parseLong("28800"))
        .build().run();
  }

  @Test
  public void testUnixTimeStampForDateWithPattern() throws Exception {
    testBuilder()
        .sqlQuery("select unix_timestamp('2009-03-20 11:30:01.0', 'yyyy-MM-dd HH:mm:ss.SSS') from cp.`employee.json` limit 1")
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValues(Long.parseLong("1237573801"))
        .build().run();

    testBuilder()
        .sqlQuery("select unix_timestamp('2009-03-20', 'yyyy-MM-dd') from cp.`employee.json` limit 1")
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValues(Long.parseLong("1237532400"))
        .build().run();
  }
}
