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

package alluxio.wire;

import alluxio.grpc.Command;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The Alluxio HeartBeat Response Message.
 */
@NotThreadSafe
public final class HeartBeatResponseMessage implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(HeartBeatResponseMessage.class);

  private static final long serialVersionUID = -8936215337909224255L;
  private Command mCommand = null;
  private Map<Long, Long> mReplicaInfo = new HashMap<>();
  private double mCompositeRatio ;
  private String mDynamicSort = "" ;
  /**
   * Creates a new instance of {@link HeartBeatResponseMessage}.
   */
  public HeartBeatResponseMessage() {
  }

  /**
   * @return the blocks to be added
   */
  public Command getCommand() {
    return mCommand;
  }

  /**
   * @return the replica info of blocks to be changed for worker
   */
  public Map<Long, Long> getReplicaInfo() {
    return mReplicaInfo;
  }
  /**
   * @return the Sort Algorithm to be changed for worker
   */
  public String getDynamicSort() {
    return mDynamicSort;}
    /**
     * @return the composite ratio of the worker
     */
    public double getCompositeRatio() { return mCompositeRatio;}
  /**
   * @param command
   * @return Set the command for heartbeat Response
   */
  public HeartBeatResponseMessage setCommand(Command command) {
    mCommand = command;
    return this;
  }

  /**
   * @param ReplicaInfo
   * @return set the replica Info to be changed
   */
  public HeartBeatResponseMessage setReplicaInfo(Map<Long, Long> ReplicaInfo) {
    mReplicaInfo = ReplicaInfo;
    return this;
  }

  public HeartBeatResponseMessage setDynamicSort(String dynamicSort) {
    Preconditions.checkNotNull(dynamicSort, "dynamicSort");
    mDynamicSort = dynamicSort;
    return this;
  }

    /**
     * @param compositeRatio
     * @return set the composite ratio of the worker
     */
    public HeartBeatResponseMessage setCompositeRatio(double compositeRatio) {
      mCompositeRatio = compositeRatio;
      return this;
    }

  /**
   * @return proto representation of the heartbeat information from master to worker
   */
  protected alluxio.grpc.BlockHeartbeatPResponse toProto() {
    return alluxio.grpc.BlockHeartbeatPResponse.newBuilder().setCommand(mCommand)
            .putAllReplicaInfo(mReplicaInfo)
            .setCompositeRatio(mCompositeRatio)
            .setDynamicSort(mDynamicSort)
            .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HeartBeatResponseMessage)) {
      return false;
    }
    HeartBeatResponseMessage that = (HeartBeatResponseMessage) o;
    return mCommand == that.mCommand && Objects.equal(mReplicaInfo, that.mReplicaInfo) && mCompositeRatio == that.mCompositeRatio
            && mDynamicSort.equals(that.mDynamicSort);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCommand, mReplicaInfo, mCompositeRatio, mDynamicSort);
  }
}
