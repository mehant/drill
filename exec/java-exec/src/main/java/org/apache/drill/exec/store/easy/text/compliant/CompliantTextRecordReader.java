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
package org.apache.drill.exec.store.easy.text.compliant;

import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.RepeatedVarCharVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class CompliantTextRecordReader extends AbstractRecordReader implements AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CompliantTextRecordReader.class);

  private static final int MAX_RECORDS_PER_BATCH = 5001;
  private static final int READ_BUFFER = 1024*1024;
  private static final int WHITE_SPACE_BUFFER = 64*1024;

  private TextParsingSettings settings;
  private FileSplit split;
  private TextReader reader;
  private DrillBuf readBuffer;
  private DrillBuf whitespaceBuffer;

  public CompliantTextRecordReader(FileSplit split, FragmentContext context, TextParsingSettings settings, List<SchemaPath> columns) {
    this.split = split;
    this.settings = settings;
    setColumns(columns);
  }

  @Override
  public boolean isStarQuery() {
    if(settings.isUseRepeatedVarChar()){
      return super.isStarQuery() || Iterables.tryFind(getColumns(), new Predicate<SchemaPath>() {
        @Override
        public boolean apply(@Nullable SchemaPath path) {
          return path.equals(RepeatedVarCharOutput.COLUMNS);
        }
      }).isPresent();
    }else{
      return isStarQuery();
    }
  }

  @Override
  public void setup(OperatorContext context, OutputMutator outputMutator) throws ExecutionSetupException {


    readBuffer = context.getManagedBuffer(READ_BUFFER);
    whitespaceBuffer = context.getManagedBuffer(WHITE_SPACE_BUFFER);

    try {
      FileSystem fs = split.getPath().getFileSystem(new Configuration());

      // raise an error if the file is compressed, new text reader does not support reading from compressed files
      CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
      CompressionCodec codec = factory.getCodec(split.getPath());
      if (codec != null) {
        throw new DrillRuntimeException("New text reader does not support reading from compressed files \n" +
            "use option  `" + ExecConstants.ENABLE_NEW_TEXT_READER_KEY + "` to switch back the text reader");
      }

      FSDataInputStream stream = fs.open(split.getPath());

      TextInput input = new TextInput(context.getStats(), settings.getNewLineDelimiter(), settings.getNormalizedNewLine(),  stream, readBuffer, split.getStart(), split.getStart() + split.getLength());

      TextOutput output = null;
      if(settings.isUseRepeatedVarChar()){
        output = new RepeatedVarCharOutput(settings, outputMutator, getColumns(), isStarQuery());
      }else{
        //TODO: Add field output.
        throw new UnsupportedOperationException();
      }

      this.reader = new TextReader(settings, input, output, whitespaceBuffer);
      reader.start();
    } catch (SchemaChangeException | IOException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    reader.resetForNextBatch();
    int cnt = 0;

    try{
      while(cnt < MAX_RECORDS_PER_BATCH && reader.parseNext()){
        cnt++;
      }
      reader.finishBatch();
      return cnt;
    }catch(IOException e){
      throw new DrillRuntimeException(e);
    }
  }

  @Override
  public void cleanup() {
    try {
      reader.close();
    } catch (IOException e) {
      logger.warn("Exception while closing stream.", e);
    }
  }

  @Override
  public void close() {
    cleanup();
  }
}
