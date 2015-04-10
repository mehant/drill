/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.hive;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileSystem;
import org.joda.time.DateTime;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

public class TestHiveStorage extends HiveTestBase {
  @Test
  public void hiveReadWithDb() throws Exception {
    test("select * from hive.kv");
  }

  @Test
  public void queryEmptyHiveTable() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.empty_table")
        .expectsEmptyResultSet()
        .go();
  }

  /**
   * Test to ensure Drill reads the all supported types correctly both normal fields (converted to Nullable types) and
   * partition fields (converted to Required types).
   * @throws Exception
   */
  @Test
  public void readAllSupportedHiveDataTypes() throws Exception {

    testBuilder().sqlQuery("SELECT * FROM hive.readtest")
        .unOrdered()
        .baselineColumns(
            "binary_field",
            "boolean_field",
            "tinyint_field",
            "decimal0_field",
            "decimal9_field",
            "decimal18_field",
            "decimal28_field",
            "decimal38_field",
            "double_field",
            "float_field",
            "int_field",
            "bigint_field",
            "smallint_field",
            "string_field",
            "varchar_field",
            "timestamp_field",
            "date_field",
            "binary_part",
            "boolean_part",
            "tinyint_part",
            "decimal0_part",
            "decimal9_part",
            "decimal18_part",
            "decimal28_part",
            "decimal38_part",
            "double_part",
            "float_part",
            "int_part",
            "bigint_part",
            "smallint_part",
            "string_part",
            "varchar_part",
            "timestamp_part",
            "date_part")
        .baselineValues(
            "binaryfield",
            false,
            (byte) 34,
            new BigDecimal("66"),
            new BigDecimal("2347.92"),
            new BigDecimal("2758725827.99990"),
            new BigDecimal("29375892739852.8"),
            new BigDecimal("89853749534593985.783"),
            8.345d,
            4.67f,
            123456,
            234235L,
            (short) 3455,
            "stringfield",
            "varcharfield",
            new DateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
            new DateTime(Date.valueOf("2013-07-05").getTime()),
            "binary",
            true,
            (byte) 64,
            new BigDecimal("370000000000"),   // TODO(DRILL-2729) Should be 37
            new BigDecimal("369000.00"),      // TODO(DRILL-2729) Should be 36.90
            new BigDecimal("-66367900898250.61888"), // TODO(DRILL-2729) Should be 3289379872.94565
            new BigDecimal("39579334534534.4"),
            new BigDecimal("363945093845093890.900"),
            8.345d,
            4.67f,
            123456,
            234235L,
            (short) 3455,
            "string",
            "varchar",
            new DateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
            new DateTime(Date.valueOf("2013-07-05").getTime()))
        .baselineValues( // All fields are null, but partition fields have non-null values
            "", // For binary (varchar, string too) empty value is considered as empty string instead of "null"
            null, null, null, null, null, null, null, null, null, null, null, null,
            "", // string_field
            "", // varchar_field
            null, null,
            "binary",
            true,
            (byte) 64,
            new BigDecimal("370000000000"),  // TODO(DRILL-2729) Should be 37
            new BigDecimal("369000.00"), // TODO(DRILL-2729) Should be 36.90
            new BigDecimal("-66367900898250.61888"), // TODO(DRILL-2729) Should be 3289379872.94565
            new BigDecimal("39579334534534.4"),
            new BigDecimal("363945093845093890.900"),
            8.345d,
            4.67f,
            123456,
            234235L,
            (short) 3455,
            "string",
            "varchar",
            new DateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
            new DateTime(Date.valueOf("2013-07-05").getTime()))
        .go();
  }

  @Test
  public void orderByOnHiveTable() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.kv ORDER BY `value` DESC")
        .ordered()
        .baselineColumns("key", "value")
        .baselineValues(5, " key_5")
        .baselineValues(4, " key_4")
        .baselineValues(3, " key_3")
        .baselineValues(2, " key_2")
        .baselineValues(1, " key_1")
        .go();
  }

  @Test
  public void queryingTablesInNonDefaultFS() throws Exception {
    // Update the default FS settings in Hive test storage plugin to non-local FS
    hiveTest.updatePluginConfig(getDrillbitContext().getStorage(),
        ImmutableMap.of(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost:9001"));

    testBuilder()
        .sqlQuery("SELECT * FROM hive.`default`.kv LIMIT 1")
        .unOrdered()
        .baselineColumns("key", "value")
        .baselineValues(1, " key_1")
        .go();
  }
}
