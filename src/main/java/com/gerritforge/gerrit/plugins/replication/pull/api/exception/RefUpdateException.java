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

import org.eclipse.jgit.lib.RefUpdate;

public class RefUpdateException extends Exception {

  private static final long serialVersionUID = 1L;
  private final RefUpdate.Result result;

  public RefUpdateException(RefUpdate.Result result, String msg) {
    super(msg);
    this.result = result;
  }

  public RefUpdate.Result getResult() {
    return result;
  }
}
