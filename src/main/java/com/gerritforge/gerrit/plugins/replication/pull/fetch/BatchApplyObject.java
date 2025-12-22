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

import com.gerritforge.gerrit.plugins.replication.pull.LocalGitRepositoryManagerProvider;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionData;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionObjectData;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingLatestPatchSetException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingParentObjectException;
import com.google.common.base.Optional;
import com.google.gerrit.entities.Project;
import com.google.gerrit.extensions.restapi.IdString;
import com.google.gerrit.extensions.restapi.ResourceNotFoundException;
import com.google.gerrit.server.git.GitRepositoryManager;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.List;
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

public class BatchApplyObject {

  private final GitRepositoryManager gitManager;

  // NOTE: We do need specifically the local GitRepositoryManager to make sure
  // to be able to write onto the directly physical repository without any wrapper.
  // Using for instance the multi-site wrapper injected by Guice would result
  // in a split-brain because of the misalignment of local vs. global refs values.
  @Inject
  public BatchApplyObject(LocalGitRepositoryManagerProvider gitManagerProvider) {
    this.gitManager = gitManagerProvider.get();
  }

  public BatchRefUpdateState applyBatch(
      Project.NameKey name, List<RefSpec> refSpecs, List<RevisionData[]> revisionsDataList)
      throws MissingParentObjectException,
          IOException,
          ResourceNotFoundException,
          MissingLatestPatchSetException {
    if (refSpecs.size() != revisionsDataList.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Mismatched batch sizes: refs=%d, revisions=%d",
              refSpecs.size(), revisionsDataList.size()));
    }

    try (Repository git = gitManager.openRepository(name)) {
      try (BatchUpdateInserter batch = BatchUpdateInserter.create(git)) {
        for (int i = 0; i < refSpecs.size(); i++) {
          batch.add(name, refSpecs.get(i), revisionsDataList.get(i));
        }
        try (RevWalk rw = new RevWalk(git)) {
          batch.getBatchRefUpdate().execute(rw, NullProgressMonitor.INSTANCE);
        }
        return new BatchRefUpdateState(batch.getBatchRefUpdate());
      }
    } catch (RepositoryNotFoundException e) {
      throw new ResourceNotFoundException(IdString.fromDecoded(name.get()), e);
    }
  }

  /**
   * Helper for building a {@link BatchRefUpdate} by inserting objects and adding {@link
   * ReceiveCommand}s.
   *
   * <p>This is intentionally separated from API/REST layers so it can be reused by batch-apply
   * paths to add multiple ref updates to the same {@link BatchRefUpdate} and execute them
   * atomically.
   */
  static class BatchUpdateInserter implements AutoCloseable {
    private final Repository git;
    private final BatchRefUpdate bru;
    private final ObjectInserter inserter;

    private BatchUpdateInserter(Repository git) {
      this.git = git;
      this.bru = git.getRefDatabase().newBatchUpdate();
      this.inserter = git.newObjectInserter();
    }

    static BatchUpdateInserter create(Repository git) {
      return new BatchUpdateInserter(git);
    }

    BatchRefUpdate getBatchRefUpdate() {
      return bru;
    }

    public void add(Project.NameKey name, RefSpec refSpec, RevisionData[] revisionsData)
        throws IOException, MissingParentObjectException, MissingLatestPatchSetException {
      ObjectId refHead = null;

      for (RevisionData revisionData : revisionsData) {

        ObjectId newObjectID = null;
        RevisionObjectData commitObject = revisionData.getCommitObject();

        if (commitObject != null) {
          RevCommit commit = RevCommit.parse(commitObject.getContent());
          for (RevCommit parent : commit.getParents()) {
            if (!git.getObjectDatabase().has(parent.getId())) {
              throw new MissingParentObjectException(name, refSpec.getSource(), parent.getId());
            }
          }

          StringBuilder error = new StringBuilder();
          // FIXME: Change refs validation to be completed
          //          if (!commitValidator.isValid(git, refSpec.getSource(), commit, error::append))
          // {
          //            throw new MissingLatestPatchSetException(name, refSpec.getSource(),
          // error.toString());
          //          }
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
          // Non-commits must be forced as they do not have a graph associated
          bru.setAllowNonFastForwards(true);
        }
      }

      ObjectId oldObjectId =
          Optional.fromNullable(git.exactRef(refSpec.getSource()))
              .transform(Ref::getObjectId)
              .or(ObjectId.zeroId());
      ReceiveCommand cmd = new ReceiveCommand(oldObjectId, refHead, refSpec.getSource());
      bru.addCommand(cmd);
    }

    @Override
    public void close() {
      inserter.close();
    }
  }
}
