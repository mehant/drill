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

/* Base class for producing output record batches while dealing with
 * Text files.
 * Currently we only produce output as a repeated varchar vector
 * with each index in the array referring to a column. A TODO is to implement
 * this interface so that we have individual columns as single varchar vectors and use
 * the information in the header to detect column names.
 */
public abstract class TextOutput {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TextOutput.class);

  private static final byte SPACE = (byte) ' ';

  public abstract void startField(int index);
  public abstract boolean endField();
  public abstract boolean endEmptyField();

  public void appendIgnoringWhitespace(byte data){
    if(TextReader.isWhite(data)){
      // noop
    }else{
      append(data);
    }
  }

  public abstract void append(byte data);
  public abstract boolean finishRecord();
  public abstract long getRecordCount();
  public abstract void startBatch();
  public abstract void finishBatch();
  public abstract boolean rowHasData();
}
