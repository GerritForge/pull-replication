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
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingLatestPatchSetException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingParentObjectException;
import com.google.gerrit.entities.Project;
import com.google.gerrit.extensions.restapi.IdString;
import com.google.gerrit.extensions.restapi.ResourceNotFoundException;
import com.google.gerrit.git.RefUpdateUtil;
import com.google.gerrit.server.git.GitRepositoryManager;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.lib.Repository;
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

  public BatchRefUpdateState apply(
      Project.NameKey name, RefSpec refSpec, RevisionData[] revisionsData)
      throws MissingParentObjectException,
          IOException,
          ResourceNotFoundException,
          MissingLatestPatchSetException {
    return applyBatch(name, List.of(refSpec), Collections.singletonList(revisionsData));
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
      try (BatchApplyObject batch = BatchApplyObject.create(git)) {
        for (int i = 0; i < refSpecs.size(); i++) {
          batch.add(name, refSpecs.get(i), revisionsDataList.get(i));
        }
        RefUpdateUtil.executeChecked(batch.getBatchRefUpdate(), git);
        return new BatchRefUpdateState(batch.getBatchRefUpdate());
      }
    } catch (RepositoryNotFoundException e) {
      throw new ResourceNotFoundException(IdString.fromDecoded(name.get()), e);
    }
  }
}
