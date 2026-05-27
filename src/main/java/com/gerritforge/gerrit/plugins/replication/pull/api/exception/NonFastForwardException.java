// Copyright (C) 2026 GerritForge, Inc.
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
import java.io.Serial;
import org.eclipse.jgit.lib.ObjectId;

/**
 * Thrown when a commit ref-update in a {@code batch-apply-object} batch would be a non-fast-forward
 * (the new commit is not a descendant of the existing tip) and the caller did not explicitly opt
 * into a forced update.
 *
 * <p>JGit's {@link org.eclipse.jgit.lib.BatchRefUpdate} only exposes a batch-wide
 * {@code setAllowNonFastForwards(boolean)} flag -- there is no per-{@link
 * org.eclipse.jgit.transport.ReceiveCommand} force-update API. {@code BatchApplyObject} flips that
 * batch-wide flag on whenever any input is a non-commit (blob) ref because blob updates are by
 * definition non-FF (blobs have no graph). To prevent the per-batch flag from silently allowing
 * non-FF updates on commit refs that share a batch with a blob ref, {@code BatchApplyObject.add}
 * pre-flight-checks commit refs with {@link org.eclipse.jgit.revwalk.RevWalk#isMergedInto} and
 * throws this exception when the new tip is not a descendant of the current tip.
 */
public class NonFastForwardException extends Exception {
  @Serial private static final long serialVersionUID = 1L;

  private final Project.NameKey projectName;
  private final String refName;
  private final ObjectId oldOid;
  private final ObjectId newOid;

  public NonFastForwardException(
      Project.NameKey projectName, String refName, ObjectId oldOid, ObjectId newOid) {
    super(
        String.format(
            "Non-fast-forward update on commit ref %s for project %s: %s -> %s"
                + " (newId is not a descendant of oldId)",
            refName, projectName, oldOid.name(), newOid.name()));
    this.projectName = projectName;
    this.refName = refName;
    this.oldOid = oldOid;
    this.newOid = newOid;
  }

  public Project.NameKey getProjectName() {
    return projectName;
  }

  public String getRefName() {
    return refName;
  }

  public ObjectId getOldOid() {
    return oldOid;
  }

  public ObjectId getNewOid() {
    return newOid;
  }
}
