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

/*******************************************************************************
 * Copyright 2014 uniVocity Software Pty Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

import io.netty.buffer.DrillBuf;
import io.netty.util.internal.PlatformDependent;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.util.AssertionUtil;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;

import com.carrotsearch.hppc.ByteArrayList;
import com.google.common.base.Charsets;
import com.univocity.parsers.common.Format;

public final class TextInput {

  private static final byte NULL_BYTE = (byte) '\0';
  private final byte lineSeparator1;
  private final byte lineSeparator2;
  private final byte normalizedLineSeparator;
  private final OperatorStats stats;

  private long lineCount;
  private long charCount;

  /**
   * The starting position in the file.
   */
  private final long startPos;
  private final long endPos;

  private int bufferMark;
  private long streamMark;
  private byte charMark;

  private long streamPos;

  private final FSDataInputStream input;
  private final DrillBuf buffer;
  private final ByteBuffer underlyingBuffer;
  private final long bStart;
  private final long bStartMinus1;

  private final boolean bufferReadable;

  /**
   * The current position in the buffer.
   */
  public int bufferPtr;

  /**
   * The quantity of valid data in the buffer.
   */
  public int length = -1;

  private boolean endFound = false;

  /**
   * Creates a new instance with the mandatory characters for handling newlines transparently.
   * @param lineSeparator the sequence of characters that represent a newline, as defined in {@link Format#getLineSeparator()}
   * @param normalizedLineSeparator the normalized newline character (as defined in {@link Format#getNormalizedNewline()}) that is used to replace any lineSeparator sequence found in the input.
   */
  public TextInput(OperatorStats stats, byte[] lineSeparator, byte normalizedLineSeparator, FSDataInputStream input, DrillBuf buffer, long startPos, long endPos) {
    if (lineSeparator == null || lineSeparator.length == 0) {
      throw new IllegalArgumentException("Invalid line separator. Expected 1 to 2 characters");
    }
    if (lineSeparator.length > 2) {
      throw new IllegalArgumentException("Invalid line separator. Up to 2 characters are expected. Got " + lineSeparator.length + " characters.");
    }
    this.stats = stats;
    this.bufferReadable = input.getWrappedStream() instanceof ByteBufferReadable;
    this.startPos = startPos;
    this.endPos = endPos;
    this.input = input;
    this.buffer = buffer;
    this.bStart = buffer.memoryAddress();
    this.bStartMinus1 = bStart -1;
    this.underlyingBuffer = buffer.nioBuffer(0, buffer.capacity());

    this.lineSeparator1 = lineSeparator[0];
    this.lineSeparator2 = lineSeparator.length == 2 ? lineSeparator[1] : NULL_BYTE;
    this.normalizedLineSeparator = normalizedLineSeparator;
  }

  public final void start() throws IOException {
//    stop();
    lineCount = 0;
    if(startPos > 0){
      input.seek(startPos);
    }

    updateBuffer();
    if (length > 0) {
      if(startPos > 0){

        // move to next full record.
        skipLines(1);
      }
    }
  }

  private void stop() throws IOException {
  }

  public String getStringSinceMarkForError() throws IOException {
    ByteArrayList bytes = new ByteArrayList();
    final long pos = getPos();
    resetToMark();
    while(getPos() < pos){
      bytes.add(nextChar());
    }
    return new String(bytes.toArray(), Charsets.UTF_8);
  }

  private long getPos(){
    return streamPos + bufferPtr;
  }

  public void mark(byte c){
    streamMark = streamPos;
    bufferMark = bufferPtr;
    charMark = c;
  }

  private final void read() throws IOException {
    if(bufferReadable){
      length = input.read(underlyingBuffer);
    }else{
      byte[] b = new byte[underlyingBuffer.capacity()];
      length = input.read(b);
      underlyingBuffer.put(b);
    }
  }

  private final void updateBuffer() throws IOException {
    streamPos = input.getPos();
    underlyingBuffer.clear();

    if(endFound){
      length = -1;
      return;
    }

    read();

    // check our data read allowance.
    if(streamPos + length >= this.endPos){
      // we've run over our alotted data.
      final byte lineSeparator1 = this.lineSeparator1;
      final byte lineSeparator2 = this.lineSeparator2;

      // find the next line separator:
      final long max = bStart + length;

      for(long m = this.bStart + (endPos - streamPos); m < max; m++){
        if(PlatformDependent.getByte(m) == lineSeparator1){
          // we found a potential line break.

          if(lineSeparator2 == NULL_BYTE){
            // we found a line separator and don't need to consult the next byte.
            length = (int)(m - bStart);
            endFound = true;
            break;
          }else{
            // this is a two byte line separator.
            long mPlus = m+1;
            if(mPlus < max){
              // we can check next byte and see if the second lineSeparator is correct.
              if(lineSeparator2 == PlatformDependent.getByte(mPlus)){
                length = (int)(mPlus - bStart);
                endFound = true;
                break;
              }else{
                // this was a partial line break.
                continue;
              }
            }else{
              // we need to read one more byte and see if it is lineSeparator
              if(lineSeparator2 == input.readByte()){
                // if it is: we actually reset the stream so that it read one byte short this time so it can read both line separators next time.
                length = (int)(m - bStart - 1);
                input.seek(streamPos + length - 1);
                streamPos--;
                break;
              }else{
                // this was a partial line break, we don't need to manage next but do need to reset stream to previous position.
                input.seek(streamPos + length);
                break;
              }
            }

          }
        }
      }

    }


    charCount += bufferPtr;
    bufferPtr = 1;

    buffer.writerIndex(underlyingBuffer.limit());
    buffer.readerIndex(underlyingBuffer.position());

    if (length == -1) {
      stop();
    }
  }


  public byte resetToMark() throws IOException{
    if(streamMark != streamPos){
      input.seek(streamMark);
      updateBuffer();
    }

    bufferPtr = bufferMark-1;
    return charMark;
  }

  public final byte nextChar() throws IOException {
    final byte lineSeparator1 = this.lineSeparator1;
    final byte lineSeparator2 = this.lineSeparator2;

    if (length == -1) {
      throw StreamFinishedPseudoException.INSTANCE;
    }

    if(AssertionUtil.BOUNDS_CHECKING_ENABLED){
      buffer.checkBytes(bufferPtr - 1, bufferPtr);
    }

    byte byteChar = PlatformDependent.getByte(bStartMinus1 + bufferPtr);

    if (bufferPtr >= length) {
      if (length != -1) {
        updateBuffer();
        bufferPtr--;
      } else {
        throw StreamFinishedPseudoException.INSTANCE;
      }
    }

    bufferPtr++;

    // monitor for next line.
    if (lineSeparator1 == byteChar && (lineSeparator2 == NULL_BYTE || lineSeparator2 == buffer.getByte(bufferPtr - 1))) {
      lineCount++;

      if (lineSeparator2 != NULL_BYTE) {
        byteChar = normalizedLineSeparator;

        if (bufferPtr >= length) {
          if (length != -1) {
            updateBuffer();
          } else {
            throw StreamFinishedPseudoException.INSTANCE;
          }
        }

//        if (bufferPtr < length) {
//          bufferPtr++;
//        }
      }
    }
    return byteChar;
  }

  public final long lineCount() {
    return lineCount;
  }

  public final void skipLines(int lines) throws IOException {
    if (lines < 1) {
      return;
    }
    long expectedLineCount = this.lineCount + lines;

    try {
      do {
        nextChar();
      } while (lineCount < expectedLineCount);
      if (lineCount < lines) {
        throw new IllegalArgumentException("Unable to skip " + lines + " lines from line " + (expectedLineCount - lines) + ". End of input reached");
      }
    } catch (EOFException ex) {
      throw new IllegalArgumentException("Unable to skip " + lines + " lines from line " + (expectedLineCount - lines) + ". End of input reached");
    }
  }

  public final long charCount() {
    return charCount + bufferPtr;
  }

  public void close() throws IOException{
    input.close();
  }
}
