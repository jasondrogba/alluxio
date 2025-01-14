/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.heartbeat;

import alluxio.conf.Reconfigurable;

/**
 * An interface for heartbeat timers. The {@link HeartbeatThread} calls the {@link #tick()} method.
 */
public interface HeartbeatTimer extends Reconfigurable {

  /**
   * When this object needs to be reconfigured
   * due to external configuration change etc.,
   * this function will be invoked.
   */
  default void update() {
  }

  /**
   * Get the interval of HeartbeatTimer.
   *
   * @return the interval of this HeartbeatTimer
   */
  default long getIntervalMs() {
    throw new UnsupportedOperationException("Getting interval is not supported");
  }

  /**
   * Waits until next heartbeat should be executed.
   *
   * @return time limit in milliseconds for this heartbeat action to run for before
   * the next heartbeat is due.
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  long tick() throws InterruptedException;
}
