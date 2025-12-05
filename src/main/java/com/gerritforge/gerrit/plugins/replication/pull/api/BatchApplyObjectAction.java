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

package com.gerritforge.gerrit.plugins.replication.pull.api;

import static com.gerritforge.gerrit.plugins.replication.pull.PullReplicationLogger.repLog;

import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionInput;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.BatchRefUpdateException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingLatestPatchSetException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingParentObjectException;
import com.gerritforge.gerrit.plugins.replication.pull.fetch.RefUpdateState;
import com.google.gerrit.extensions.restapi.PreconditionFailedException;
import com.google.gerrit.extensions.restapi.ResourceConflictException;
import com.google.gerrit.extensions.restapi.Response;
import com.google.gerrit.extensions.restapi.RestApiException;
import com.google.gerrit.extensions.restapi.RestModifyView;
import com.google.gerrit.extensions.restapi.UnprocessableEntityException;
import com.google.gerrit.server.project.ProjectResource;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
class BatchApplyObjectAction implements RestModifyView<ProjectResource, List<RevisionInput>> {

  private final ApplyObjectCommand applyObjectCommand;

  @Inject
  BatchApplyObjectAction(ApplyObjectCommand applyObjectCommand) {
    this.applyObjectCommand = applyObjectCommand;
  }

  @Override
  public Response<?> apply(ProjectResource resource, List<RevisionInput> inputs)
      throws RestApiException {

    String batchInputStr =
        inputs.stream().map(RevisionInput::toString).collect(Collectors.joining(","));
    repLog.info(
        "Batch Apply object API for project {} for refs {}", resource.getNameKey(), batchInputStr);

    // TODO: All pre-flight checks

    try {
      List<RefUpdateState> refUpdateStates =
          applyObjectCommand.batchApplyObject(resource.getNameKey(), inputs);
      return Response.ok(refUpdateStates);
    } catch (MissingParentObjectException e) {
      repLog.error(
          "Batch Apply object for project {} *FAILED*: {}",
          resource.getNameKey(),
          batchInputStr,
          e);
      throw new ResourceConflictException(e.getMessage(), e);
    } catch (NumberFormatException | IOException e) {
      repLog.error(
          "Batch Apply object for project {} *FAILED*: {}",
          resource.getNameKey(),
          batchInputStr,
          e);
      throw RestApiException.wrap(e.getMessage(), e);
    } catch (BatchRefUpdateException e) {
      repLog.error(
          "Batch Apply object for project {} *FAILED*: {}",
          resource.getNameKey(),
          batchInputStr,
          e);
      throw new UnprocessableEntityException(e.getMessage());
    } catch (MissingLatestPatchSetException e) {
      repLog.error(
          "Batch Apply object for project {} *FAILED*: {}",
          resource.getNameKey(),
          batchInputStr,
          e);
      throw new PreconditionFailedException(e.getMessage());
    }
  }
}
