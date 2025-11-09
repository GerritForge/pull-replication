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

package com.gerritforge.gerrit.plugins.replication.pull.fetch;

import org.eclipse.jgit.lib.RefUpdate;

public class RefUpdateState {

  private String remoteName;
  private RefUpdate.Result result;

  public RefUpdateState(String remoteName, RefUpdate.Result result) {
    this.remoteName = remoteName;
    this.result = result;
  }

  public String getRemoteName() {
    return remoteName;
  }

  public RefUpdate.Result getResult() {
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("RefUpdateState[");
    sb.append(remoteName);
    sb.append(" ");
    sb.append(result);
    sb.append("]");
    return sb.toString();
  }
}
