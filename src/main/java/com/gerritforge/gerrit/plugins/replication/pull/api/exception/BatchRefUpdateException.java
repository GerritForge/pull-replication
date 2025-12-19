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

package com.gerritforge.gerrit.plugins.replication.pull.api.exception;

import com.gerritforge.gerrit.plugins.replication.pull.fetch.BatchRefUpdateState;
import java.io.Serial;
import org.eclipse.jgit.transport.ReceiveCommand;

public class BatchRefUpdateException extends Exception {
  @Serial private static final long serialVersionUID = 1L;

  private final BatchRefUpdateState state;

  public BatchRefUpdateException(BatchRefUpdateState state, String message) {
    super(message);
    this.state = state;
  }

  public boolean containsRejectedResult() {
    return state.getCommands().stream()
        .anyMatch(
            c ->
                c.getResult() == ReceiveCommand.Result.REJECTED_NOCREATE
                    || c.getResult() == ReceiveCommand.Result.REJECTED_NODELETE
                    || c.getResult() == ReceiveCommand.Result.REJECTED_NONFASTFORWARD);
  }

  @Override
  public String toString() {
    return state.toLogLine();
  }
}
