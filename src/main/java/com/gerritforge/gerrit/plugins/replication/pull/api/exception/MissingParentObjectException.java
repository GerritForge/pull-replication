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

import com.google.gerrit.entities.Project;
import org.eclipse.jgit.lib.ObjectId;

public class MissingParentObjectException extends Exception {
  private static final long serialVersionUID = 1L;

  public MissingParentObjectException(
      Project.NameKey project, String refName, ObjectId parentObjectId) {
    super(
        String.format(
            "Missing parent object %s for project %s ref name: %s",
            parentObjectId.getName(), project.get(), refName));
  }

  public MissingParentObjectException(Project.NameKey project, String refName, String targetName) {
    super(
        String.format(
            "Missing parent object on %s for project %s ref name: %s",
            targetName, project.get(), refName));
  }
}
