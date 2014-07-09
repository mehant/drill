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
1:      
2:      package org.apache.drill.exec.test.generated;
3:      
4:      import org.apache.drill.exec.exception.SchemaChangeException;
5:      import org.apache.drill.exec.expr.holders.NullableBitHolder;
6:      import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
7:      import org.apache.drill.exec.expr.holders.VarCharHolder;
8:      import org.apache.drill.exec.ops.FragmentContext;
9:      import org.apache.drill.exec.record.RecordBatch;
10:     import org.apache.drill.exec.vector.NullableVarCharVector;
11:     import org.apache.drill.exec.vector.ValueHolderHelper;
12:     
13:     public class FiltererGen0 {
14:     
15:         NullableVarCharVector vv0;
16:         VarCharHolder string4;
17:         VarCharHolder constant5;
18:     
19:         public void doSetup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing)
20:             throws SchemaChangeException
21:         {
22:             {
23:                 int[] fieldIds1 = new int[ 1 ] ;
24:                 fieldIds1 [ 0 ] = 0;
25:                 Object tmp2 = (incoming).getValueAccessorById(NullableVarCharVector.class, fieldIds1).getValueVector();
26:                 if (tmp2 == null) {
27:                     throw new SchemaChangeException("Failure while loading vector vv0 with id: org.apache.drill.exec.record.TypedFieldId@283c3e27.");
28:                 }
29:                 vv0 = ((NullableVarCharVector) tmp2);
30:                 string4 = ValueHolderHelper.getVarCharHolder("James Compagno");
31:                 constant5 = string4;
32:                 /** start SETUP for function equal **/ 
33:                 {
34:                     VarCharHolder right = constant5;
35:                      {}
36:                 }
37:                 /** end SETUP for function equal **/ 
38:             }
39:         }
40:     
41:         public boolean doEval(int inIndex, int outIndex)
42:             throws SchemaChangeException
43:         {
44:             {
45:                 NullableVarCharHolder out3 = new NullableVarCharHolder();
46:                 out3 .isSet = vv0 .getAccessor().isSet((inIndex));
47:                 if (out3 .isSet == 1) {
48:                     {
49:                         vv0 .getAccessor().get((inIndex), out3);
50:                     }
51:                 }
52:                 //---- start of eval portion of equal function. ----//
53:                 NullableBitHolder out6 = new NullableBitHolder();
54:                 {
55:                     if (out3 .isSet == 0) {
56:                         out6 .isSet = 0;
57:                     } else {
58:                         final NullableBitHolder out = new NullableBitHolder();
59:                         NullableVarCharHolder left = out3;
60:                         VarCharHolder right = constant5;
61:                          
62:     GCompareVarCharVarChar$EqualsVarCharVarChar_eval: {
63:         outside:
64:         {
65:             if (left.end - left.start == right.end - right.start) {
66:                 int n = left.end - left.start;
67:                 int l = left.start;
68:                 int r = right.start;
69:     
70:                 while (n-- != 0) {
71:                     byte leftByte = left.buffer.getByte(l++);
72:                     byte rightByte = right.buffer.getByte(r++);
73:     
74:                     if (leftByte != rightByte) {
75:                         out.value = 0;
76:                         break outside;
77:                     }
78:                 }
79:                 out.value = 1;
80:             } else
81:             {
82:                 out.value = 0;
83:             }
84:         }
85:     }
86:      
87:                         out.isSet = 1;
88:                         out6 = out;
89:                         out.isSet = 1;
90:                     }
91:                 }
92:                 //---- end of eval portion of equal function. ----//
93:                 return (out6 .value == 1);
94:             }
95:         }
96:     
97:     }

