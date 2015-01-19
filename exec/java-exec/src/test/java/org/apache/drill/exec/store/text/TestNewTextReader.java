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
package org.apache.drill.exec.store.text;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestNewTextReader extends BaseTestQuery {

  @Test
  public void testFieldDelimiterWithinQuotes() throws Exception {
    testBuilder()
        .sqlQuery("select columns[1] as col1 from cp.`textinput/input1.csv`")
        .unOrdered()
        .optionSettingQueriesForTestQuery("alter session set `exec.storage.enable_new_text_reader` = true")
        .baselineColumns("col1")
        .baselineValues("foo,bar")
        .go();
  }

  @Test
  public void testNewLineDelimeterWithingQuotes() throws Exception {
    try {
    testBuilder()
        .sqlQuery("select columns[1] as col1 from cp.`textinput/input2.csv`")
        .unOrdered()
        .optionSettingQueriesForTestQuery("alter session set `exec.storage.enable_new_text_reader` = true")
        .baselineColumns("col1")
        .baselineValues("foo,bar")
        .go();
    } catch (Exception e) {
      assert e.getMessage().contains("Cannot use newline character within quoted string");
      return;
    }
    throw new Exception("Test framework verification failed");
  }

  @Test
  public void testNewLine() throws Exception {
    test("alter session set `exec.storage.enable_new_text_reader` = true; select columns[1] as col1 from cp.`textinput/input2.csv`");
  }
}
