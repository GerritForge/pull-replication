// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.gerritforge.gerrit.plugins.replication.pull;

import com.google.gerrit.entities.Project;

/** Thrown when a synchronous fetch cannot acquire the lock for its project. */
public class ProjectLockFailureException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final String conflictingTaskId;

  public ProjectLockFailureException(
      Project.NameKey project, String conflictingTaskId, Throwable cause) {
    super(
        "Unable to lock project "
            + project
            + " because it is already locked by task "
            + conflictingTaskId,
        cause);
    this.conflictingTaskId = conflictingTaskId;
  }

  public String getConflictingTaskId() {
    return conflictingTaskId;
  }
}
