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
package org.apache.drill.exec.expr.fn.impl;

import com.google.common.base.Charsets;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal9Holder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.IntervalDayHolder;
import org.apache.drill.exec.expr.holders.IntervalHolder;
import org.apache.drill.exec.expr.holders.IntervalYearHolder;
import org.apache.drill.exec.expr.holders.SmallIntHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.TinyIntHolder;
import org.apache.drill.exec.expr.holders.UInt1Holder;
import org.apache.drill.exec.expr.holders.UInt2Holder;
import org.apache.drill.exec.expr.holders.UInt4Holder;
import org.apache.drill.exec.expr.holders.UInt8Holder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.impl.SingleMapReaderImpl;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.util.DecimalUtility;

import io.netty.buffer.DrillBuf;

import org.joda.time.Period;

import java.util.Iterator;

public class MappifyUtility {

  // Default names used in the map.
  private static final String fieldKey = "key";
  private static final String fieldValue = "value";

  public static void mappify(FieldReader reader, BaseWriter.ComplexWriter writer, DrillBuf buffer) {
    // Currently we expect single map as input
    if (!(reader instanceof SingleMapReaderImpl)) {
      throw new DrillRuntimeException("Mappify function only supports Simple maps as input");
    }
    BaseWriter.ListWriter listWriter = writer.rootAsList();
    listWriter.start();
    BaseWriter.MapWriter mapWriter = listWriter.map();

    // Iterate over the fields in the map
    Iterator<String> fieldIterator = reader.iterator();
    while (fieldIterator.hasNext()) {
      String str = fieldIterator.next();
      FieldReader fieldReader = reader.reader(str);

      // Skip the field if its null
      if (fieldReader.isSet() == false) {
        continue;
      }

      // Check if the value field is not repeated
      if (fieldReader.getType().getMode() == TypeProtos.DataMode.REPEATED) {
        throw new DrillRuntimeException("Mappify function does not support repeated type values");
      }

      // writing a new field, start a new map
      mapWriter.start();

      // write "key":"columnname" into the map
      VarCharHolder vh = new VarCharHolder();
      byte[] b = str.getBytes(Charsets.UTF_8);
      buffer.reallocIfNeeded(b.length);
      buffer.setBytes(0, b);
      vh.start = 0;
      vh.end = b.length;
      vh.buffer = buffer;
      mapWriter.varChar(fieldKey).write(vh);

      // Write the value to the map
      writeToMap(fieldReader, mapWriter, buffer);

      mapWriter.end();
    }
    listWriter.end();
  }

  /*
   * Function to read a value from the field reader, detect the type, construct the appropriate value holder
   * and use the value holder to write to the Map.
   *
   * Currently this logic is very specific for this use case, although since its already separated out
   * it can be moved into a utility method if it needs to be shared.
   */
  public static void writeToMap(FieldReader fieldReader, BaseWriter.MapWriter mapWriter, DrillBuf buffer) {

    TypeProtos.MajorType valueMajorType = fieldReader.getType();
    TypeProtos.MinorType valueType = valueMajorType.getMinorType();
    if (valueType == TypeProtos.MinorType.BIGINT) {
      BigIntHolder bh = new BigIntHolder();
      bh.value = fieldReader.readLong();
      mapWriter.bigInt(fieldValue).write(bh);
    } else if (valueType == TypeProtos.MinorType.INT) {
      IntHolder ih = new IntHolder();
      ih.value = fieldReader.readInteger();
      mapWriter.integer(fieldValue).write(ih);
    } else if (valueType == TypeProtos.MinorType.TINYINT) {
      TinyIntHolder tinyIntHolder = new TinyIntHolder();
      tinyIntHolder.value = fieldReader.readByte();
      mapWriter.tinyInt(fieldValue).write(tinyIntHolder);
    } else if (valueType == TypeProtos.MinorType.VARCHAR) {
      VarCharHolder vh1 = new VarCharHolder();
      byte[] b = fieldReader.readText().toString().getBytes(Charsets.UTF_8);
      buffer.reallocIfNeeded(b.length);
      buffer.setBytes(0, b);
      vh1.start = 0;
      vh1.end = b.length;
      vh1.buffer = buffer;
      mapWriter.varChar(fieldValue).write(vh1);
    } else if (valueType == TypeProtos.MinorType.SMALLINT) {
      SmallIntHolder smallIntHolder = new SmallIntHolder();
      smallIntHolder.value = fieldReader.readShort();
      mapWriter.smallInt(fieldValue).write(smallIntHolder);
    } else if (valueType == TypeProtos.MinorType.DECIMAL9) {
      Decimal9Holder decimalHolder = new Decimal9Holder();
      decimalHolder.value = fieldReader.readBigDecimal().intValue();
      decimalHolder.scale = valueMajorType.getScale();
      decimalHolder.precision = valueMajorType.getPrecision();
      mapWriter.decimal9(fieldValue).write(decimalHolder);
    } else if (valueType == TypeProtos.MinorType.DECIMAL18) {
      Decimal18Holder decimalHolder = new Decimal18Holder();
      decimalHolder.value = fieldReader.readBigDecimal().longValue();
      decimalHolder.scale = valueMajorType.getScale();
      decimalHolder.precision = valueMajorType.getPrecision();
      mapWriter.decimal18(fieldValue).write(decimalHolder);
    } else if (valueType == TypeProtos.MinorType.DECIMAL28SPARSE) {
      Decimal28SparseHolder decimalHolder = new Decimal28SparseHolder();

      // Ensure that the buffer used to store decimal is of sufficient length
      buffer.reallocIfNeeded(decimalHolder.WIDTH);
      decimalHolder.scale = valueMajorType.getScale();
      decimalHolder.precision = valueMajorType.getPrecision();
      decimalHolder.buffer = buffer;
      decimalHolder.start = 0;
      DecimalUtility.getSparseFromBigDecimal(fieldReader.readBigDecimal(), buffer, 0, decimalHolder.scale,
                                             decimalHolder.precision, decimalHolder.nDecimalDigits);
      mapWriter.decimal28Sparse(fieldValue).write(decimalHolder);
    } else if (valueType == TypeProtos.MinorType.DECIMAL38SPARSE) {
      Decimal38SparseHolder decimalHolder = new Decimal38SparseHolder();

      // Ensure that the buffer used to store decimal is of sufficient length
      buffer.reallocIfNeeded(decimalHolder.WIDTH);
      decimalHolder.scale = valueMajorType.getScale();
      decimalHolder.precision = valueMajorType.getPrecision();
      decimalHolder.buffer = buffer;
      decimalHolder.start = 0;
      DecimalUtility.getSparseFromBigDecimal(fieldReader.readBigDecimal(), buffer, 0, decimalHolder.scale,
          decimalHolder.precision, decimalHolder.nDecimalDigits);

      mapWriter.decimal38Sparse(fieldValue).write(decimalHolder);

    } else if (valueType == TypeProtos.MinorType.DATE) {
      DateHolder dateHolder = new DateHolder();
      dateHolder.value = fieldReader.readLong();
      mapWriter.date(fieldValue).write(dateHolder);
    } else if (valueType == TypeProtos.MinorType.TIME) {
      TimeHolder timeHolder = new TimeHolder();
      timeHolder.value = fieldReader.readInteger();
      mapWriter.time(fieldValue).write(timeHolder);
    } else if (valueType == TypeProtos.MinorType.TIMESTAMP) {
      TimeStampHolder timeStampHolder = new TimeStampHolder();
      timeStampHolder.value = fieldReader.readLong();
      mapWriter.timeStamp(fieldValue).write(timeStampHolder);
    } else if (valueType == TypeProtos.MinorType.INTERVAL) {
      IntervalHolder intervalHolder = new IntervalHolder();
      Period period = fieldReader.readPeriod();
      intervalHolder.months = (period.getYears() * org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths) + period.getMonths();
      intervalHolder.days = period.getDays();
      intervalHolder.milliseconds = (period.getHours() * org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis) +
          (period.getMinutes() * org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis) +
          (period.getSeconds() * org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis) +
          (period.getMillis());
      mapWriter.interval(fieldValue).write(intervalHolder);
    } else if (valueType == TypeProtos.MinorType.INTERVALDAY) {
      IntervalDayHolder intervalDayHolder = new IntervalDayHolder();
      Period period = fieldReader.readPeriod();
      intervalDayHolder.days = period.getDays();
      intervalDayHolder.milliseconds = (period.getHours() * org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis) +
          (period.getMinutes() * org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis) +
          (period.getSeconds() * org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis) +
          (period.getMillis());
      mapWriter.intervalDay(fieldValue).write(intervalDayHolder);
    } else if (valueType == TypeProtos.MinorType.INTERVALYEAR) {
      IntervalYearHolder intervalYearHolder = new IntervalYearHolder();
      Period period = fieldReader.readPeriod();
      intervalYearHolder.value = (period.getYears() * org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths) + period.getMonths();
      mapWriter.intervalYear(fieldValue).write(intervalYearHolder);
    } else if (valueType == TypeProtos.MinorType.FLOAT4) {
      Float4Holder float4Holder = new Float4Holder();
      float4Holder.value = fieldReader.readFloat();
      mapWriter.float4(fieldValue).write(float4Holder);
    } else if (valueType == TypeProtos.MinorType.FLOAT8) {
      Float8Holder float8Holder = new Float8Holder();
      float8Holder.value = fieldReader.readDouble();
      mapWriter.float8(fieldValue).write(float8Holder);
    } else if (valueType == TypeProtos.MinorType.BIT) {
      BitHolder bitHolder = new BitHolder();
      bitHolder.value = (fieldReader.readBoolean() == true) ? 1 : 0;
      mapWriter.bit(fieldValue).write(bitHolder);
    } else if (valueType == TypeProtos.MinorType.VARBINARY) {
      VarBinaryHolder varBinaryHolder = new VarBinaryHolder();
      byte[] b = fieldReader.readByteArray();
      buffer.reallocIfNeeded(b.length);
      buffer.setBytes(0, b);
      varBinaryHolder.start = 0;
      varBinaryHolder.end = b.length;
      varBinaryHolder.buffer = buffer;
      mapWriter.varBinary(fieldValue).write(varBinaryHolder);
    } else if (valueType == TypeProtos.MinorType.UINT1) {
      UInt1Holder uInt1Holder = new UInt1Holder();
      uInt1Holder.value = fieldReader.readByte();
      mapWriter.uInt1(fieldValue).write(uInt1Holder);
    } else if (valueType == TypeProtos.MinorType.UINT2) {
      UInt2Holder uInt2Holder = new UInt2Holder();
      uInt2Holder.value = fieldReader.readCharacter();
      mapWriter.uInt2(fieldValue).write(uInt2Holder);
    } else if (valueType == TypeProtos.MinorType.UINT4) {
      UInt4Holder uInt4Holder = new UInt4Holder();
      uInt4Holder.value = fieldReader.readInteger();
      mapWriter.uInt4(fieldValue).write(uInt4Holder);
    } else if (valueType == TypeProtos.MinorType.UINT8) {
      UInt8Holder uInt8Holder = new UInt8Holder();
      uInt8Holder.value = fieldReader.readInteger();
      mapWriter.uInt8(fieldValue).write(uInt8Holder);
    } else if (valueType == TypeProtos.MinorType.MAP || valueType == TypeProtos.MinorType.LIST ||
               valueType == TypeProtos.MinorType.LATE) {
      throw new DrillRuntimeException("Mappify function only supports scalar values");
    }
  }
}

