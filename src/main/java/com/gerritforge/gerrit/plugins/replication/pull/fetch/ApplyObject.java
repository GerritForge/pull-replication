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
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionInput;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionObjectData;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingLatestPatchSetException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingParentObjectException;
import com.google.common.base.MoreObjects;
import com.google.gerrit.entities.Project;
import com.google.gerrit.extensions.restapi.IdString;
import com.google.gerrit.extensions.restapi.ResourceNotFoundException;
import com.google.gerrit.git.RefUpdateUtil;
import com.google.gerrit.server.git.GitRepositoryManager;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.List;
import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.BatchRefUpdate;
import org.eclipse.jgit.lib.NullProgressMonitor;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.ReceiveCommand;
import org.eclipse.jgit.transport.RefSpec;

public class ApplyObject {

  private final GitRepositoryManager gitManager;

  // NOTE: We do need specifically the local GitRepositoryManager to make sure
  // to be able to write onto the directly physical repository without any wrapper.
  // Using for instance the multi-site wrapper injected by Guice would result
  // in a split-brain because of the misalignment of local vs. global refs values.
  @Inject
  public ApplyObject(LocalGitRepositoryManagerProvider gitManagerProvider) {
    this.gitManager = gitManagerProvider.get();
  }

  public RefUpdateState apply(Project.NameKey name, RefSpec refSpec, RevisionData[] revisionsData)
      throws MissingParentObjectException,
          IOException,
          ResourceNotFoundException,
          MissingLatestPatchSetException {
    try (Repository git = gitManager.openRepository(name)) {

      ObjectId refHead = null;
      BatchRefUpdate batchRefUpdate = git.getRefDatabase().newBatchUpdate();
      try (ObjectInserter oi = git.newObjectInserter()) {
        String refName = refSpec.getSource();
        for (RevisionData revisionData : revisionsData) {

          ObjectId newObjectID = null;
          RevisionObjectData commitObject = revisionData.getCommitObject();

          if (commitObject != null) {
            RevCommit commit = RevCommit.parse(commitObject.getContent());
            for (RevCommit parent : commit.getParents()) {
              if (!git.getObjectDatabase().has(parent.getId())) {
                throw new MissingParentObjectException(name, refName, parent.getId());
              }
            }

            StringBuffer error = new StringBuffer();
            if (!ChangeMetaCommitValidator.isValid(git, refName, commit, error::append)) {
              throw new MissingLatestPatchSetException(name, refName, error.toString());
            }
          }

          for (RevisionObjectData rev : revisionData.getBlobs()) {
            ObjectId blobObjectId = oi.insert(rev.getType(), rev.getContent());
            if (newObjectID == null) {
              newObjectID = blobObjectId;
            }
            refHead = newObjectID;
          }

          if (commitObject != null) {
            RevisionObjectData treeObject = revisionData.getTreeObject();
            oi.insert(treeObject.getType(), treeObject.getContent());

            refHead = oi.insert(commitObject.getType(), commitObject.getContent());
          }

          oi.flush();

          if (commitObject == null) {
            // Non-commits must be forced as they do not have a graph associated
            // ru.setForceUpdate(true);
            // TODO: How do I set this to bru?
          }
        }

        ObjectId oldId = MoreObjects.firstNonNull(git.resolve(refName), ObjectId.zeroId());
        batchRefUpdate.addCommand(new ReceiveCommand(oldId, refHead, refName));
        RefUpdateUtil.executeChecked(batchRefUpdate, git);
        return new RefUpdateState(refName, RefUpdate.Result.FORCED);
      }
    } catch (RepositoryNotFoundException e) {
      throw new ResourceNotFoundException(IdString.fromDecoded(name.get()), e);
    }
  }

  public List<RefUpdateState> apply(Project.NameKey name, List<RevisionInput> inputs)
      throws IOException,
          MissingParentObjectException,
          MissingLatestPatchSetException,
          ResourceNotFoundException {
    try (Repository git = gitManager.openRepository(name)) {

      BatchRefUpdate batchRefUpdate = git.getRefDatabase().newBatchUpdate();

      for (RevisionInput input : inputs) {
        ObjectId refHead = null;
        String refName = new RefSpec(input.getRefName()).getSource();
        try (ObjectInserter oi = git.newObjectInserter()) {
          RevisionData revisionData = input.getRevisionData();

          ObjectId newObjectID = null;
          RevisionObjectData commitObject = revisionData.getCommitObject();

          if (commitObject != null) {
            RevCommit commit = RevCommit.parse(commitObject.getContent());
            for (RevCommit parent : commit.getParents()) {
              if (!git.getObjectDatabase().has(parent.getId())) {
                throw new MissingParentObjectException(name, refName, parent.getId());
              }
            }

            StringBuilder error = new StringBuilder();
            if (!ChangeMetaCommitValidator.isValid(git, refName, commit, error::append)) {
              throw new MissingLatestPatchSetException(name, refName, error.toString());
            }
          }

          for (RevisionObjectData rev : revisionData.getBlobs()) {
            ObjectId blobObjectId = oi.insert(rev.getType(), rev.getContent());
            if (newObjectID == null) {
              newObjectID = blobObjectId;
            }
            refHead = newObjectID;
          }

          if (commitObject != null) {
            RevisionObjectData treeObject = revisionData.getTreeObject();
            oi.insert(treeObject.getType(), treeObject.getContent());

            refHead = oi.insert(commitObject.getType(), commitObject.getContent());
          }

          oi.flush();

          if (commitObject == null) {
            // Non-commits must be forced as they do not have a graph associated
            // ru.setForceUpdate(true);
            // TODO: How do I set this to bru?
          }
        }

        ObjectId oldId = MoreObjects.firstNonNull(git.resolve(refName), ObjectId.zeroId());
        batchRefUpdate.addCommand(new ReceiveCommand(oldId, refHead, refName));
      }
      batchRefUpdate.execute(rw, NullProgressMonitor.INSTANCE);
      RefUpdateUtil.executeChecked(batchRefUpdate, git);
      return batchRefUpdate.getCommands().stream()
          .map(c -> new RefUpdateState(c.getRefName(), decode(c)))
          .toList();
    } catch (RepositoryNotFoundException e) {
      throw new ResourceNotFoundException(IdString.fromDecoded(name.get()), e);
    }
  }

  private static RefUpdate.Result decode(ReceiveCommand receiveCommand) {
    return switch (receiveCommand.getResult()) {
      case OK -> {
        if (AnyObjectId.isEqual(receiveCommand.getOldId(), receiveCommand.getNewId()))
          yield RefUpdate.Result.NO_CHANGE;
        yield switch (receiveCommand.getType()) {
          case CREATE -> RefUpdate.Result.NEW;
          case UPDATE -> RefUpdate.Result.FAST_FORWARD;
          default -> RefUpdate.Result.FORCED;
        };
      }
      case REJECTED_NOCREATE, REJECTED_NODELETE, REJECTED_NONFASTFORWARD ->
          RefUpdate.Result.REJECTED;
      case REJECTED_CURRENT_BRANCH -> RefUpdate.Result.REJECTED_CURRENT_BRANCH;
      case REJECTED_MISSING_OBJECT -> RefUpdate.Result.IO_FAILURE;
      default -> RefUpdate.Result.LOCK_FAILURE;
    };
  }
}
