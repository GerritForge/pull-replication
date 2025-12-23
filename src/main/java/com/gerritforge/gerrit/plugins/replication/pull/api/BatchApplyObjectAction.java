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
import static com.gerritforge.gerrit.plugins.replication.pull.api.ApplyObjectCommand.ApplyInvocationType.BATCH_APPLY_OBJECT;
import static com.google.common.base.Preconditions.checkState;

import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionData;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionInput;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.BatchRefUpdateException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingLatestPatchSetException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingParentObjectException;
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
import java.util.List;
import java.util.stream.Collectors;

@Singleton
class BatchApplyObjectAction implements RestModifyView<ProjectResource, List<RevisionInput>> {

  private final ApplyObjectCommand applyObjectCommand;
  private final ApplyObjectInputValidator inputValidator;

  @Inject
  BatchApplyObjectAction(
      ApplyObjectCommand applyObjectCommand, ApplyObjectInputValidator inputValidator) {
    this.inputValidator = inputValidator;
    this.applyObjectCommand = applyObjectCommand;
  }

  @Override
  public Response<?> apply(ProjectResource resource, List<RevisionInput> inputs)
      throws RestApiException {
    Project.NameKey projectNameKey = resource.getNameKey();

    for (RevisionInput input : inputs) {
      inputValidator.validate(projectNameKey, input);
    }

    List<String> labels = inputs.stream().map(RevisionInput::getLabel).distinct().toList();
    List<Long> eventCreatedOns =
        inputs.stream().map(RevisionInput::getEventCreatedOn).distinct().toList();

    checkState(
        labels.size() == 1,
        "Label for %s was expected to be exactly 1, but %s were found: [%s]",
        BATCH_APPLY_OBJECT,
        labels.size(),
        String.join(",", labels));

    checkState(
        eventCreatedOns.size() == 1,
        "eventCreatedOn for %s was expected to be exactly 1, but %s were found: [%s]",
        BATCH_APPLY_OBJECT,
        eventCreatedOns.size(),
        String.join(
            ",", eventCreatedOns.stream().map(Object::toString).collect(Collectors.joining(","))));

    List<String> refNames = inputs.stream().map(RevisionInput::getRefName).toList();
    List<RevisionData[]> revisionData =
        inputs.stream().map(r -> new RevisionData[] {r.getRevisionData()}).toList();
    String label = labels.getFirst();
    Long eventCreatedOn = eventCreatedOns.getFirst();

    String revisionsForLog =
        inputs.stream().map(r -> r.getRevisionData().toString()).collect(Collectors.joining(","));

    try {
      applyObjectCommand.batchApplyObjects(
          BATCH_APPLY_OBJECT, projectNameKey, refNames, revisionData, label, eventCreatedOn);

      return Response.ok();
    } catch (MissingParentObjectException e) {
      repLog.error(
          "{} API *FAILED* from {} for {}:{} - {}",
          BATCH_APPLY_OBJECT,
          label,
          projectNameKey,
          refNames,
          revisionsForLog,
          e);
      throw new ResourceConflictException(e.getMessage(), e);
    } catch (NumberFormatException | IOException e) {
      repLog.error(
          "{} API *FAILED* from {} for {}:{} - {}",
          BATCH_APPLY_OBJECT,
          label,
          projectNameKey,
          refNames,
          revisionsForLog,
          e);
      throw RestApiException.wrap(e.getMessage(), e);
    } catch (BatchRefUpdateException e) {
      if (refNames.stream().anyMatch(RefNames::isRefsDraftsComments)
          && e.containsRejectedResult()) {
        repLog.info(
            "{} API *REJECTED* from {} for {}:{} - {}",
            BATCH_APPLY_OBJECT,
            label,
            projectNameKey,
            refNames,
            revisionsForLog);
      } else {
        repLog.error(
            "{} API *FAILED* from {} for {}:{} - {}",
            BATCH_APPLY_OBJECT,
            label,
            projectNameKey,
            refNames,
            revisionsForLog,
            e);
      }
      throw new UnprocessableEntityException(e.getMessage());
    } catch (MissingLatestPatchSetException e) {
      repLog.error(
          "{} API *FAILED* from {} for {}:{} - {}",
          BATCH_APPLY_OBJECT,
          label,
          projectNameKey,
          refNames,
          revisionsForLog,
          e);
      throw new PreconditionFailedException(e.getMessage());
    }
  }
}
