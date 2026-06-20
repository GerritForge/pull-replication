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

package com.gerritforge.gerrit.plugins.replication.pull.api;

import static com.gerritforge.gerrit.plugins.replication.pull.PullReplicationLogger.repLog;

import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionInput;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingLatestPatchSetException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingParentObjectException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.RefUpdateException;
import com.google.gerrit.entities.Project;
import com.google.gerrit.entities.RefNames;
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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.eclipse.jgit.lib.RefUpdate;

@Singleton
class BatchApplyObjectAction implements RestModifyView<ProjectResource, List<RevisionInput>> {

  private final ApplyObjectCommand applyObjectCommand;
  private final BatchApplyObjectInputValidator inputValidator;

  @Inject
  BatchApplyObjectAction(
      ApplyObjectCommand applyObjectCommand, BatchApplyObjectInputValidator inputValidator) {
    this.applyObjectCommand = applyObjectCommand;
    this.inputValidator = inputValidator;
  }

  @Override
  public Response<?> apply(ProjectResource resource, List<RevisionInput> inputs)
      throws RestApiException {
    Project.NameKey projectNameKey = resource.getNameKey();

    repLog.info(
        "Batch Apply object API from {} for refs {}",
        projectNameKey,
        inputs.stream().map(RevisionInput::getRefName).collect(Collectors.joining(",")));

    List<Response<?>> allResponses = new ArrayList<>();
    for (RevisionInput input : inputs) {
      allResponses.add(applyOne(projectNameKey, input));
    }
    return Response.ok(allResponses);
  }

  // Per-input handling matches ApplyObjectAction.apply byte-for-byte in
  // semantics (validation, logging, exception mapping). Inlined here to
  // break the dependency on ApplyObjectAction. The atomicity behavior is
  // unchanged: each input is applied by its own RefUpdate via
  // ApplyObjectCommand.applyObject; a failure mid-batch leaves the
  // already-applied refs in place. Atomicity is addressed in the
  // follow-up commit that introduces BatchApplyObject.
  private Response<?> applyOne(Project.NameKey projectNameKey, RevisionInput input)
      throws RestApiException {
    inputValidator.validate(projectNameKey, input);

    repLog.info(
        "Apply object API from {} for {}:{} - {}",
        input.getLabel(),
        projectNameKey,
        input.getRefName(),
        input.getRevisionData());

    try {
      applyObjectCommand.applyObject(
          projectNameKey,
          input.getRefName(),
          input.getRevisionData(),
          input.getLabel(),
          input.getEventCreatedOn());
      return Response.created();
    } catch (MissingParentObjectException e) {
      repLog.error(
          "Apply object API *FAILED* from {} for {}:{} - {}",
          input.getLabel(),
          projectNameKey,
          input.getRefName(),
          input.getRevisionData(),
          e);
      throw new ResourceConflictException(e.getMessage(), e);
    } catch (NumberFormatException | IOException e) {
      repLog.error(
          "Apply object API *FAILED* from {} for {}:{} - {}",
          input.getLabel(),
          projectNameKey,
          input.getRefName(),
          input.getRevisionData(),
          e);
      throw RestApiException.wrap(e.getMessage(), e);
    } catch (RefUpdateException e) {
      if (RefNames.isRefsDraftsComments(input.getRefName())
          && e.getResult().equals(RefUpdate.Result.REJECTED)) {
        repLog.info(
            "Apply object API *REJECTED* from {} for {}:{} - {}",
            input.getLabel(),
            projectNameKey,
            input.getRefName(),
            input.getRevisionData());
      } else {
        repLog.error(
            "Apply object API *FAILED* from {} for {}:{} - {}",
            input.getLabel(),
            projectNameKey,
            input.getRefName(),
            input.getRevisionData(),
            e);
      }
      throw new UnprocessableEntityException(e.getMessage());
    } catch (MissingLatestPatchSetException e) {
      repLog.error(
          "Apply object API *FAILED* from {} for {}:{} - {}",
          input.getLabel(),
          projectNameKey,
          input.getRefName(),
          input.getRevisionData(),
          e);
      throw new PreconditionFailedException(e.getMessage());
    }
  }
}
