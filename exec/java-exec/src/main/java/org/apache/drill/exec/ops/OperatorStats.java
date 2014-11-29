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
package org.apache.drill.exec.ops;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.MetricValue;
import org.apache.drill.exec.proto.UserBitShared.OperatorProfile;
import org.apache.drill.exec.proto.UserBitShared.StreamProfile;
import org.apache.drill.exec.util.AssertionUtil;

import com.carrotsearch.hppc.IntDoubleOpenHashMap;
import com.carrotsearch.hppc.IntLongOpenHashMap;

public class OperatorStats {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorStats.class);


  private final Wait wait = new Wait();
  private final Setup setup = new Setup();
  private final Processing processing = new Processing();

  protected final int operatorId;
  protected final int operatorType;
  private final BufferAllocator allocator;

  private IntLongOpenHashMap longMetrics = new IntLongOpenHashMap();
  private IntDoubleOpenHashMap doubleMetrics = new IntDoubleOpenHashMap();

  public long[] recordsReceivedByInput;
  public long[] batchesReceivedByInput;
  private long[] schemaCountByInput;


  private boolean inProcessing = false;
  private boolean inSetup = false;
  private boolean inWait = false;

  protected long processingNanos;
  protected long setupNanos;
  protected long waitNanos;

  private long processingMark;
  private long setupMark;
  private long waitMark;

  private boolean previouslyInSetup;

  private long schemas;

  public OperatorStats(OpProfileDef def, BufferAllocator allocator){
    this(def.getOperatorId(), def.getOperatorType(), def.getIncomingCount(), allocator);
  }

  private OperatorStats(int operatorId, int operatorType, int inputCount, BufferAllocator allocator) {
    super();
    this.allocator = allocator;
    this.operatorId = operatorId;
    this.operatorType = operatorType;
    this.recordsReceivedByInput = new long[inputCount];
    this.batchesReceivedByInput = new long[inputCount];
    this.schemaCountByInput = new long[inputCount];
  }

  private String assertionError(String msg){
    return String.format("Failure while %s for operator id %d. Currently have states of processing:%s, setup:%s, waiting:%s.", msg, operatorId, inProcessing, inSetup, inWait);
  }
  public Setup startSetup() {
    assert !inSetup  : assertionError("starting setup");
    stopProcessing();
    inSetup = true;
    setupMark = System.nanoTime();
    return setup;
  }

  public void stopSetup() {
    assert inSetup :  assertionError("stopping setup");
    startProcessing();
    setupNanos += elapsed(setupMark);
    inSetup = false;
  }

  public Processing startProcessing() {
    assert !inProcessing : assertionError("starting processing");
    processingMark = System.nanoTime();
    inProcessing = true;
    return processing;
  }

  public void stopProcessing() {
    assert inProcessing : assertionError("stopping processing");
    processingNanos += elapsed(processingMark);
    inProcessing = false;
  }

  public Wait startWait() {
    assert !inWait : assertionError("starting waiting");
    stopProcessing();
    inWait = true;
    waitMark = System.nanoTime();
    return wait;
  }

  private static long elapsed(long mark){
    long val = System.nanoTime() - mark;
    if(DrillConfig.ON_OSX && val < 0){
      return 0;
    }
    if(val > 10000000000l){
      System.out.println("");
    }
    return val;
  }

  public void stopWait() {
    assert inWait : assertionError("stopping waiting");
    startProcessing();
    waitNanos += elapsed(waitMark);
    inWait = false;
  }

  public void batchReceived(int inputIndex, long records, boolean newSchema) {
    recordsReceivedByInput[inputIndex] += records;
    batchesReceivedByInput[inputIndex]++;
    if(newSchema){
      schemaCountByInput[inputIndex]++;
    }
  }

  public OperatorProfile getProfile() {
    final OperatorProfile.Builder b = OperatorProfile //
        .newBuilder() //
        .setOperatorType(operatorType) //
        .setOperatorId(operatorId) //
        .setSetupNanos(setupNanos) //
        .setProcessNanos(processingNanos)
        .setWaitNanos(waitNanos);

    if(allocator != null){
      b.setPeakLocalMemoryAllocated(allocator.getPeakMemoryAllocation());
    }



    addAllMetrics(b);

    return b.build();
  }

  public void addAllMetrics(OperatorProfile.Builder builder) {
    addStreamProfile(builder);
    addLongMetrics(builder);
    addDoubleMetrics(builder);
  }

  public void addStreamProfile(OperatorProfile.Builder builder) {
    for(int i = 0; i < recordsReceivedByInput.length; i++){
      builder.addInputProfile(StreamProfile.newBuilder().setBatches(batchesReceivedByInput[i]).setRecords(recordsReceivedByInput[i]).setSchemas(this.schemaCountByInput[i]));
    }
  }

  public void addLongMetrics(OperatorProfile.Builder builder) {
    for(int i =0; i < longMetrics.allocated.length; i++){
      if(longMetrics.allocated[i]){
        builder.addMetric(MetricValue.newBuilder().setMetricId(longMetrics.keys[i]).setLongValue(longMetrics.values[i]));
      }
    }
  }

  public void addDoubleMetrics(OperatorProfile.Builder builder) {
    for(int i =0; i < doubleMetrics.allocated.length; i++){
      if(doubleMetrics.allocated[i]){
        builder.addMetric(MetricValue.newBuilder().setMetricId(doubleMetrics.keys[i]).setDoubleValue(doubleMetrics.values[i]));
      }
    }
  }

  public void addLongStat(MetricDef metric, long value){
    longMetrics.putOrAdd(metric.metricId(), value, value);
  }

  public void addDoubleStat(MetricDef metric, double value){
    doubleMetrics.putOrAdd(metric.metricId(), value, value);
  }

  public void setLongStat(MetricDef metric, long value){
    longMetrics.put(metric.metricId(), value);
  }

  public void setDoubleStat(MetricDef metric, double value){
    doubleMetrics.put(metric.metricId(), value);
  }

  public class Setup implements AutoCloseable{
    @Override
    public void close(){
      stopSetup();
    }
  }

  public class Wait implements AutoCloseable{
    @Override
    public void close(){
      stopWait();
    }
  }

  public class Processing implements AutoCloseable{
    @Override
    public void close(){
      stopProcessing();
    }
  }

}
