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

package com.gerritforge.gerrit.plugins.replication.pull.fetch;

import com.gerritforge.gerrit.plugins.replication.pull.LocalGitRepositoryManagerProvider;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionData;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionObjectData;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingLatestPatchSetException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingParentObjectException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.NonFastForwardException;
import com.google.gerrit.entities.Project;
import com.google.gerrit.extensions.restapi.IdString;
import com.google.gerrit.extensions.restapi.ResourceNotFoundException;
import com.google.gerrit.server.git.GitRepositoryManager;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.lib.BatchRefUpdate;
import org.eclipse.jgit.lib.NullProgressMonitor;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.transport.ReceiveCommand;
import org.eclipse.jgit.transport.RefSpec;

/**
 * Multi-ref atomic apply for the batch-apply-object path.
 *
 * <p>Deliberately separate from {@link ApplyObject}: the single-ref path stays on {@link
 * org.eclipse.jgit.lib.RefUpdate} (atomic by definition for one ref) and incurs no {@link
 * BatchRefUpdate} overhead. All {@code BatchRefUpdate} machinery is confined to this class.
 *
 * <p>Snapshot-vs-execute window: {@link #applyBatch} reads each ref's current OID via {@code
 * git.exactRef(...)} when constructing the {@link org.eclipse.jgit.transport.ReceiveCommand}, then
 * runs {@code bru.execute(rw, NullProgressMonitor.INSTANCE)} in the same try-with-resources. If a
 * concurrent writer advances any of those refs between snapshot and execute, JGit's
 * {@code BatchRefUpdate} compares the snapshotted {@code oldId} against the current ref state and
 * rejects the affected command with {@code LOCK_FAILURE} (or {@code REJECTED_OTHER_REASON} when
 * the batch is atomic — default on every backend we run on). That JGit invariant is what gives us
 * the TOCTOU guarantee; we don't pin it directly here because reproducing the race from outside
 * this class would require either re-exposing the internal {@code BatchRefUpdate} or a latched
 * concurrent writer. The atomicity contracts we own (pre-flight parent / FF / force-flag scope)
 * are pinned by the {@code BatchApplyObject*IT} family.
 */
public class BatchApplyObject {

  private final GitRepositoryManager gitManager;

  // Use the local GitRepositoryManager explicitly; the multi-site wrapper would
  // route writes through global-refdb consensus and cause split-brain on the
  // replication apply path. Mirrors the reasoning in ApplyObject.
  @Inject
  public BatchApplyObject(LocalGitRepositoryManagerProvider gitManagerProvider) {
    this.gitManager = gitManagerProvider.get();
  }

  public BatchRefUpdateState applyBatch(
      Project.NameKey name, List<RefSpec> refSpecs, List<RevisionData[]> revisionsDataList)
      throws MissingParentObjectException,
          IOException,
          ResourceNotFoundException,
          MissingLatestPatchSetException,
          NonFastForwardException {
    if (refSpecs.size() != revisionsDataList.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Mismatched batch sizes: refs=%d, revisions=%d",
              refSpecs.size(), revisionsDataList.size()));
    }

    try (Repository git = gitManager.openRepository(name)) {
      BatchRefUpdate bru = git.getRefDatabase().newBatchUpdate();
      try (ObjectInserter inserter = git.newObjectInserter()) {
        for (int i = 0; i < refSpecs.size(); i++) {
          addCommand(git, bru, inserter, name, refSpecs.get(i), revisionsDataList.get(i));
        }
        try (RevWalk rw = new RevWalk(git)) {
          bru.execute(rw, NullProgressMonitor.INSTANCE);
        }
        return new BatchRefUpdateState(bru);
      }
    } catch (RepositoryNotFoundException e) {
      throw new ResourceNotFoundException(IdString.fromDecoded(name.get()), e);
    }
  }

  private static void addCommand(
      Repository git,
      BatchRefUpdate bru,
      ObjectInserter inserter,
      Project.NameKey name,
      RefSpec refSpec,
      RevisionData[] revisionsData)
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

    bru.addCommand(new ReceiveCommand(oldObjectId, refHead, refSpec.getSource()));
  }
}
