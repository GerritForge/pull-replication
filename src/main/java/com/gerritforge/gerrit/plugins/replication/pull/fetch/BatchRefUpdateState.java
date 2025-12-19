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

import java.util.List;
import java.util.stream.Collectors;
import org.eclipse.jgit.lib.BatchRefUpdate;
import org.eclipse.jgit.transport.ReceiveCommand;

public class BatchRefUpdateState {

  private final BatchRefUpdate bru;

  public BatchRefUpdateState(BatchRefUpdate bru) {
    this.bru = bru;
  }

  public List<ReceiveCommand> getCommands() {
    return bru.getCommands();
  }

  public List<ReceiveCommand.Result> getResults() {
    return getCommands().stream().map(ReceiveCommand::getResult).toList();
  }

  public boolean isSuccessful() {
    return getResults().stream().allMatch(r -> r == ReceiveCommand.Result.OK);
  }

  public String toLogLine() {
    return String.format(
        "BatchRefUpdate[ok=%s, total=%d, results=%s]",
        isSuccessful(), getCommands().size(), formatCommands());
  }

  private String formatCommands() {
    return getCommands().stream()
        .map(c -> c.getRefName() + ":" + c.getResult())
        .collect(Collectors.joining(", "));
  }

  @Override
  public String toString() {
    return "BatchRefUpdateState[" + bru + "]";
  }
}
