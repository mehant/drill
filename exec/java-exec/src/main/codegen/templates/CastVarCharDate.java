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

<@pp.dropOutputFile />

<#list cast.types as type>
<#if type.major == "VarCharDate">  <#-- Template to convert from VarChar to Date, Time, TimeStamp, TimeStampTZ -->

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gcast/Cast${type.from}To${type.to}.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl.gcast;

import io.netty.buffer.ByteBuf;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionCostCategory;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;
import org.joda.time.MutableDateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.DateMidnight;
import org.apache.drill.exec.expr.fn.impl.DateUtility;

@SuppressWarnings("unused")
@FunctionTemplate(names = {"cast${type.to?upper_case}", "${type.alias}"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL, 
  costCategory = FunctionCostCategory.COMPLEX)
public class Cast${type.from}To${type.to} implements DrillSimpleFunc {

  @Param ${type.from}Holder in;
  <#if type.to == "Date">
  @Workspace org.joda.time.chrono.ISOChronology c;
  </#if>
  @Output ${type.to}Holder out;

  public void setup(RecordBatch incoming) {
    <#if type.to == "Date">
    c = org.joda.time.chrono.ISOChronology.getInstanceUTC();
    </#if>
  }

  public void eval() {
      <#if type.to == "Date">
      int index = in.start;
      int endIndex = in.end;
      int digit = 0;
      int radix = 10; // Base 10 digits

      // Stores three fields (year, month, day)
      int[] dateFields = new int[3];
      int dateIndex = 0;
      int value = 0;

      while (dateIndex < 3 && index < endIndex) {
        digit = Character.digit(in.buffer.getByte(index++), radix);

        if (digit == -1) {
          dateFields[dateIndex++] = value;
          value = 0;
        } else {
          value = (value * 10) + digit;
        }
      }

      if (dateIndex < 3) {
        // If we reached the end of input, we would have not encountered a separator, store the last value
        dateFields[dateIndex++] = value;
      }

      /* Handle two digit years
       * Follow convention as done by Oracle, Postgres
       * If range of two digits between 70 - 99 then year = 1970 - 1999
       * Else if two digits between 00 - 69 = 2000 - 2069
       */
      if (dateFields[0] < 100) {
        if (dateFields[0] < 70) {
          dateFields[0] += 2000;
        } else {
          dateFields[0] += 1900;
        }
      }

      out.value = c.getDateTimeMillis(dateFields[0], dateFields[1], dateFields[2], 0);

      <#else>
      byte[] buf = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
      String input = new String(buf, com.google.common.base.Charsets.UTF_8);

      <#if type.to == "TimeStamp">
      org.joda.time.format.DateTimeFormatter f = org.apache.drill.exec.expr.fn.impl.DateUtility.getDateTimeFormatter();
      out.value = org.joda.time.DateTime.parse(input, f).withZoneRetainFields(org.joda.time.DateTimeZone.UTC).getMillis();

      <#elseif type.to == "TimeStampTZ">
      org.joda.time.format.DateTimeFormatter f = org.apache.drill.exec.expr.fn.impl.DateUtility.getDateTimeFormatter();
      org.joda.time.DateTime dt = org.joda.time.DateTime.parse(input, f);
      out.value = dt.getMillis();
      out.index = org.apache.drill.exec.expr.fn.impl.DateUtility.getIndex(dt.getZone().toString());

      <#elseif type.to == "Time">
      org.joda.time.format.DateTimeFormatter f = org.apache.drill.exec.expr.fn.impl.DateUtility.getTimeFormatter();
      out.value = (int) ((f.parseDateTime(input)).withZoneRetainFields(org.joda.time.DateTimeZone.UTC).getMillis());
      </#if>
      </#if>

  }
}
</#if> <#-- type.major -->
</#list>
