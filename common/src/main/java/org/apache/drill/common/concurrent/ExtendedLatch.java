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
package org.apache.drill.common.concurrent;

import java.util.concurrent.CountDownLatch;

/**
 * An extended CountDownLatch which allows us to await uninterruptibly.
 */
public class ExtendedLatch extends CountDownLatch {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExtendedLatch.class);

  public ExtendedLatch() {
    super(1);
  }

  public ExtendedLatch(final int count) {
    super(count);
  }

  /**
   * Returns whether or not interruptions should continue to be ignored. This can be overridden in subclasses to check a
   * state variable or similar.
   *
   * @return Whether awaitUninterruptibly() should continue ignoring interruptions.
   */
  protected boolean ignoreInterruptions() {
    return true;
  }

  /**
   * Await without interruption. In the case of interruption, log a warning and continue to wait. This also checks the
   * output of ignoreInterruptions();
   */
  public void awaitUninterruptibly() {
    boolean ready = false;
    while (!ready) {
      try {
        await();
        ready = true;
      } catch (final InterruptedException e) {
        if (ignoreInterruptions()) {
          // if we're still not ready, the while loop will cause us to wait again
          logger.warn("Interrupted while waiting for event latch.", e);
        } else {
          return;
        }
      }
    }
  }
}
