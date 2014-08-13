/*******************************************************************************

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
 ******************************************************************************/
package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.DrillBuf;

public class StringFunctionHelpers {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StringFunctionHelpers.class);

  // Assumes Alpha as [A-Za-z0-9]
  // white space is treated as everything else.
  public static void initCap(int start, int end, DrillBuf inBuf, DrillBuf outBuf){
    boolean capNext = true;

    for (int id = start; id < end; id++) {
      byte  currentByte = inBuf.getByte(id);

      // 'A - Z' : 0x41 - 0x5A
      // 'a - z' : 0x61 - 0x7A
      // '0-9'   : 0x30 - 0x39
      if (capNext) {  // curCh is whitespace or first character of word.
        if (currentByte >= 0x30 && currentByte <= 0x39) { // 0-9
          capNext = false;
        } else if (currentByte >=0x41 && currentByte <= 0x5A) {  // A-Z
          capNext = false;
        } else if (currentByte >= 0x61 && currentByte <= 0x7A) {  // a-z
          capNext = false;
          currentByte -= 0x20; // Uppercase this character
        }
        // else {} whitespace
      } else {  // Inside of a word or white space after end of word.
        if (currentByte >= 0x30 && currentByte <= 0x39) { // 0-9
          // noop
        } else if (currentByte >=0x41 && currentByte <= 0x5A) {  // A-Z
          currentByte -= 0x20 ; // Lowercase this character
        } else if (currentByte >= 0x61 && currentByte <= 0x7A) {  // a-z
          // noop
        } else { // whitespace
          capNext = true;
        }
      }

      outBuf.setByte(id, currentByte) ;
    } //end of for_loop
  }

}
