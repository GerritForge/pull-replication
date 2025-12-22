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

import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionData;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionObjectData;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingLatestPatchSetException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingParentObjectException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.NonFastForwardException;
import com.google.gerrit.entities.Project;
import java.io.IOException;
import java.util.Optional;
import org.eclipse.jgit.lib.BatchRefUpdate;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.transport.ReceiveCommand;
import org.eclipse.jgit.transport.RefSpec;

/**
 * Helper for building a {@link BatchRefUpdate} by inserting objects and adding {@link
 * ReceiveCommand}s.
 *
 * <p>This is intentionally separated from API/REST layers so it can be reused by batch-apply paths
 * to add multiple ref updates to the same {@link BatchRefUpdate} and execute them atomically.
 */
class BatchApplyObject implements AutoCloseable {
  private final Repository git;
  private final BatchRefUpdate bru;
  private final ObjectInserter inserter;

  private BatchApplyObject(Repository git) {
    this.git = git;
    this.bru = git.getRefDatabase().newBatchUpdate();
    this.inserter = git.newObjectInserter();
  }

  static BatchApplyObject create(Repository git) {
    return new BatchApplyObject(git);
  }

  BatchRefUpdate getBatchRefUpdate() {
    return bru;
  }

  public void add(Project.NameKey name, RefSpec refSpec, RevisionData[] revisionsData)
      throws IOException,
          MissingParentObjectException,
          MissingLatestPatchSetException,
          NonFastForwardException {
    ObjectId refHead = null;
    boolean isCommitRef = false;

    for (RevisionData revisionData : revisionsData) {

      ObjectId newObjectID = null;
      RevisionObjectData commitObject = revisionData.getCommitObject();

      if (commitObject != null) {
        isCommitRef = true;
        RevCommit commit = RevCommit.parse(commitObject.getContent());
        for (RevCommit parent : commit.getParents()) {
          if (!git.getObjectDatabase().has(parent.getId())) {
            throw new MissingParentObjectException(name, refSpec.getSource(), parent.getId());
          }
        }

        StringBuilder error = new StringBuilder();
        if (!ChangeMetaCommitValidator.isValid(git, refSpec.getSource(), commit, error::append)) {
          throw new MissingLatestPatchSetException(name, refSpec.getSource(), error.toString());
        }
      }

      for (RevisionObjectData rev : revisionData.getBlobs()) {
        ObjectId blobObjectId = inserter.insert(rev.getType(), rev.getContent());
        if (newObjectID == null) {
          newObjectID = blobObjectId;
        }
        refHead = newObjectID;
      }

      if (commitObject != null) {
        RevisionObjectData treeObject = revisionData.getTreeObject();
        inserter.insert(treeObject.getType(), treeObject.getContent());

        refHead = inserter.insert(commitObject.getType(), commitObject.getContent());
      }

      inserter.flush();

      if (commitObject == null) {
        // Non-commits must be forced as they do not have a graph associated.
        // JGit's BatchRefUpdate has no per-ReceiveCommand force API, so this is
        // batch-wide. We compensate below by pre-flight-checking commit refs
        // for non-FF and rejecting them before they reach bru.execute().
        bru.setAllowNonFastForwards(true);
      }
    }

    ObjectId oldObjectId =
        Optional.ofNullable(git.exactRef(refSpec.getSource()))
            .map(Ref::getObjectId)
            .orElse(ObjectId.zeroId());

    // Pre-flight FF check on commit refs. The per-batch setAllowNonFastForwards
    // flag set above for blob inputs would otherwise leak into commit-ref
    // commands in the same batch and silently allow non-FF updates on them.
    // JGit applies isAllowNonFastForwards() uniformly per command (see
    // BatchRefUpdate.java:656), so we enforce the per-ref intent here.
    if (isCommitRef && !ObjectId.zeroId().equals(oldObjectId)) {
      try (RevWalk rw = new RevWalk(git)) {
        RevCommit oldCommit = rw.parseCommit(oldObjectId);
        RevCommit newCommit = rw.parseCommit(refHead);
        if (!rw.isMergedInto(oldCommit, newCommit)) {
          throw new NonFastForwardException(name, refSpec.getSource(), oldObjectId, refHead);
        }
      }
    }

    ReceiveCommand cmd = new ReceiveCommand(oldObjectId, refHead, refSpec.getSource());
    bru.addCommand(cmd);
  }

  @Override
  public void close() {
    inserter.close();
  }
}
