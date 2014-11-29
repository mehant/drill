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

import com.univocity.parsers.common.CommonParserSettings;
import com.univocity.parsers.common.TextParsingException;
import com.univocity.parsers.common.processor.RowProcessor;
import com.univocity.parsers.csv.CsvParserSettings;

/*******************************************************************************
 * Portions Copyright 2014 uniVocity Software Pty Ltd
 ******************************************************************************/

/**
 * A very fast Text parser implementation.  Builds heavily upon the uniVocity parsers.  Customized for UTF8 parsing and DrillBuf support.
 */
public final class TextReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TextReader.class);

  private static final byte NULL_BYTE = (byte) '\0';

  private final TextParsingContext context;

  private final long recordsToRead;
  private final TextParsingSettings settings;

  private final TextInput input;
  private final TextOutput output;
  private final DrillBuf workBuf;

  private byte ch;
  private int fieldIndex;

  /** Behavior settings **/
  private final boolean ignoreTrailingWhitespace;
  private final boolean ignoreLeadingWhitespace;
  private final boolean parseUnescapedQuotes;

  /** Key Characters **/
  private final byte comment;
  private final byte delimiter;
  private final byte quote;
  private final byte quoteEscape;
  private final byte newLine;
  private int fieldSize;

  /**
   * The CsvParser supports all settings provided by {@link CsvParserSettings}, and requires this configuration to be
   * properly initialized.
   *
   * @param settings
   *          the parser configuration
   */
  public TextReader(TextParsingSettings settings, TextInput input, TextOutput output, DrillBuf workBuf) {
    this.context = new TextParsingContext(input, output);
    this.workBuf = workBuf;
    this.settings = settings;

    this.recordsToRead = settings.getNumberOfRecordsToRead() == -1 ? Long.MAX_VALUE : settings.getNumberOfRecordsToRead();

    this.ignoreTrailingWhitespace = settings.isIgnoreTrailingWhitespaces();
    this.ignoreLeadingWhitespace = settings.isIgnoreLeadingWhitespaces();
    this.parseUnescapedQuotes = settings.isParseUnescapedQuotes();
    this.delimiter = settings.getDelimiter();
    this.quote = settings.getQuote();
    this.quoteEscape = settings.getQuoteEscape();
    this.newLine = settings.getNormalizedNewLine();
    this.comment = settings.getComment();

    this.input = input;
    this.output = output;

  }

  public TextOutput getOutput(){
    return output;
  }

  static final boolean isWhite(byte b){
    return b <= ' ' && b > -1;
  }

  public void resetForNextBatch(){
    output.startBatch();
  }

  private boolean parseRecord() throws IOException {
    final byte newLine = this.newLine;
    final TextInput input = this.input;

    input.mark(ch);

    fieldIndex = 0;
    if (isWhite(ch) && ignoreLeadingWhitespace) {
      skipWhitespace();
    }

    int fieldsWritten = 0;
    try{
      boolean earlyTerm = false;
      while (ch != newLine) {
        earlyTerm = !parseField();
        fieldsWritten++;
        if (ch != newLine) {
          ch = input.nextChar();
          if (ch == newLine) {
            output.endEmptyField();
            break;
          }
        }
        if(earlyTerm){
          if(ch != newLine){
            input.skipLines(1);
          }
          break;
        }
      }
    }catch(StreamFinishedPseudoException e){
      // if we've written part of a field or all of a field, we should send this row.
      if(fieldsWritten == 0 && !output.rowHasData()){
        throw e;
      }
    }

    if(output.finishRecord()){
      return true;
    }else{
      ch = input.resetToMark();
      return false;
    }

  }

  private void parseValueIgnore() throws IOException {
    final byte newLine = this.newLine;
    final byte delimiter = this.delimiter;
    final TextOutput output = this.output;
    final TextInput input = this.input;

    byte ch = this.ch;
    while (ch != delimiter && ch != newLine) {
      output.appendIgnoringWhitespace(ch);
//      fieldSize++;
      ch = input.nextChar();
    }
    this.ch = ch;
  }

  private void parseValueAll() throws IOException {
    final byte newLine = this.newLine;
    final byte delimiter = this.delimiter;
    final TextOutput output = this.output;
    final TextInput input = this.input;

    byte ch = this.ch;
    while (ch != delimiter && ch != newLine) {
      output.append(ch);
//      fieldSize++;
      ch = input.nextChar();
    }
    this.ch = ch;
  }

  private void parseValue() throws IOException {
    if (ignoreTrailingWhitespace) {
      parseValueIgnore();
    }else{
      parseValueAll();
    }
  }

  private void parseQuotedValue(byte prev) throws IOException {
    final byte newLine = this.newLine;
    final byte delimiter = this.delimiter;
    final TextOutput output = this.output;
    final TextInput input = this.input;
    final byte quote = this.quote;

    ch = input.nextChar();

    while (!(prev == quote && (ch == delimiter || ch == newLine || isWhite(ch)))) {
      if (ch != quote) {
        if (prev == quote) { // unescaped quote detected
          if (parseUnescapedQuotes) {
            output.append(quote);
            output.append(ch);
            parseQuotedValue(ch);
            break;
          } else {
            throw new TextParsingException(
                context,
                "Unescaped quote character '"
                    + quote
                    + "' inside quoted value of CSV field. To allow unescaped quotes, set 'parseUnescapedQuotes' to 'true' in the CSV parser settings. Cannot parse CSV input.");
          }
        }
        output.append(ch);
        prev = ch;
      } else if (prev == quoteEscape) {
        output.append(quote);
        prev = NULL_BYTE;
      } else {
        prev = ch;
      }
      ch = input.nextChar();
    }

    // handles whitespaces after quoted value: whitespaces are ignored. Content after whitespaces may be parsed if
    // 'parseUnescapedQuotes' is enabled.
    if (ch != newLine && ch <= ' ') {
      final DrillBuf workBuf = this.workBuf;
      workBuf.resetWriterIndex();
      do {
        // saves whitespaces after value
        workBuf.writeByte(ch);
        ch = input.nextChar();
        // found a new line, go to next record.
        if (ch == newLine) {
          return;
        }
      } while (ch <= ' ');

      // there's more stuff after the quoted value, not only empty spaces.
      if (!(ch == delimiter || ch == newLine) && parseUnescapedQuotes) {

        output.append(quote);
        for(int i =0; i < workBuf.writerIndex(); i++){
          output.append(workBuf.getByte(i));
        }
        // the next character is not the escape character, put it there
        if (ch != quoteEscape) {
          output.append(ch);
        }
        // sets this character as the previous character (may be escaping)
        // calls recursively to keep parsing potentially quoted content
        parseQuotedValue(ch);
      }
    }

    if (!(ch == delimiter || ch == newLine)) {
      throw new TextParsingException(context, "Unexpected character '" + ch
          + "' following quoted value of CSV field. Expecting '" + delimiter + "'. Cannot parse CSV input.");
    }
  }

  private final boolean parseField() throws IOException {

    output.startField(fieldIndex++);

    if (isWhite(ch) && ignoreLeadingWhitespace) {
      skipWhitespace();
    }

    if (ch == delimiter) {
      return output.endEmptyField();
    } else {
      if (ch == quote) {
        parseQuotedValue(NULL_BYTE);
      } else {
        parseValue();
      }

      return output.endField();
    }

  }

  private void skipWhitespace() throws IOException {
    final byte delimiter = this.delimiter;
    final byte newLine = this.newLine;
    final TextInput input = this.input;

    while (isWhite(ch) && ch != delimiter && ch != newLine) {
      ch = input.nextChar();
    }
  }

  /**
   * Parses the entirety of a given input and delegates each parsed row to an instance of {@link RowProcessor}, defined
   * by {@link CommonParserSettings#getRowProcessor()}.
   *
   * @param reader
   *          The input to be parsed.
   * @throws IOException
   */
  public final void start() throws IOException {
    context.stopped = false;
    input.start();
  }


  /**
   * Parses the next record from the input.
   */
  public final boolean parseNext() throws IOException {
    try {
      while (!context.stopped) {
        ch = input.nextChar();
        if (ch == comment) {
          input.skipLines(1);
          continue;
        }
        break;
      }

      boolean success = parseRecord();

      if(success){
        if (recordsToRead > 0 && context.currentRecord() >= recordsToRead) {
          context.stop();
        }
        return true;
      }else{
        return false;
      }

    } catch (StreamFinishedPseudoException ex) {
      stopParsing();
      return false;
    } catch (Exception ex) {
      try {
        throw handleException(ex);
      } finally {
        stopParsing();
      }
    }
  }

  private void stopParsing(){

  }

  private String displayLineSeparators(String str, boolean addNewLine) {
    if (addNewLine) {
      if (str.contains("\r\n")) {
        str = str.replaceAll("\\r\\n", "[\\\\r\\\\n]\r\n\t");
      } else if (str.contains("\n")) {
        str = str.replaceAll("\\n", "[\\\\n]\n\t");
      } else {
        str = str.replaceAll("\\r", "[\\\\r]\r\t");
      }
    } else {
      str = str.replaceAll("\\n", "\\\\n");
      str = str.replaceAll("\\r", "\\\\r");
    }
    return str;
  }

  private TextParsingException handleException(Exception ex) throws IOException {
    String message = null;
    String tmp = input.getStringSinceMarkForError();
    char[] chars = tmp.toCharArray();
    if (chars != null) {
      int length = chars.length;
      if (length > settings.getMaxCharsPerColumn()) {
        message = "Length of parsed input (" + length
            + ") exceeds the maximum number of characters defined in your parser settings ("
            + settings.getMaxCharsPerColumn() + "). ";
      }

      if (tmp.contains("\n") || tmp.contains("\r")) {
        tmp = displayLineSeparators(tmp, true);
        String lineSeparator = displayLineSeparators(settings.getLineSeparatorString(), false);
        message += "\nIdentified line separator characters in the parsed content. This may be the cause of the error. The line separator in your parser settings is set to '"
            + lineSeparator + "'. Parsed content:\n\t" + tmp;
      }

      int nullCharacterCount = 0;
      // ensuring the StringBuilder won't grow over Integer.MAX_VALUE to avoid OutOfMemoryError
      int maxLength = length > Integer.MAX_VALUE / 2 ? Integer.MAX_VALUE / 2 - 1 : length;
      StringBuilder s = new StringBuilder(maxLength);
      for (int i = 0; i < maxLength; i++) {
        if (chars[i] == '\0') {
          s.append('\\');
          s.append('0');
          nullCharacterCount++;
        } else {
          s.append(chars[i]);
        }
      }
      tmp = s.toString();

      if (nullCharacterCount > 0) {
        message += "\nIdentified "
            + nullCharacterCount
            + " null characters ('\0') on parsed content. This may indicate the data is corrupt or its encoding is invalid. Parsed content:\n\t"
            + tmp;
      }

    }

    throw new TextParsingException(context, message, ex);
  }

  public void finishBatch(){
    output.finishBatch();
  }

  public void close() throws IOException{
    input.close();
  }

}
