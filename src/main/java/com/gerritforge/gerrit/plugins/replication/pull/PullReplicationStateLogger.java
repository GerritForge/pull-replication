// Copyright (C) 2025 GerritForge, Inc.
//
// Licensed under the BSL 1.1 (the "License");
// you may not use this file except in compliance with the License.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.gerritforge.gerrit.plugins.replication.pull;

import static com.gerritforge.gerrit.plugins.replication.pull.PullReplicationLogger.repLog;

import com.google.inject.Singleton;

/**
 * Wrapper around a Logger that also logs out the replication state.
 *
 * <p>When logging replication errors it is useful to know the current replication state. This
 * utility class wraps the methods from Logger and logs additional information about the replication
 * state to the stderr console.
 */
@Singleton
public class PullReplicationStateLogger implements ReplicationStateListener {

  @Override
  public void warn(String msg, ReplicationState... states) {
    stateWriteErr("Warning: " + msg, states);
    repLog.warn(msg);
  }

  @Override
  public void error(String msg, ReplicationState... states) {
    stateWriteErr("Error: " + msg, states);
    repLog.error(msg);
  }

  @Override
  public void error(String msg, Throwable t, ReplicationState... states) {
    stateWriteErr("Error: " + msg, states);
    repLog.error(msg, t);
  }

  private void stateWriteErr(String msg, ReplicationState[] states) {
    if (states == null) {
      return;
    }

    for (ReplicationState rs : states) {
      if (rs != null) {
        rs.writeStdErr(msg);
      }
    }
  }
}
